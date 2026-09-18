#!/usr/bin/env python3
"""
Genera un notebook PySpark (.ipynb) para Microsoft Fabric a partir del IR
transpilado (salida de transpile_formula.py, que a su vez extiende la salida
de parse_workflow.py).

Uso:
    python scripts/generate_notebook.py flujo.transpiled.json
    python scripts/generate_notebook.py flujo.transpiled.json --out flujo.ipynb --modo strict --lakehouse-name VentasLH

Estructura del notebook: SETUP / INPUTS / READ / TRANSFORMS / QA / OUTPUTS /
LOGGING (ver references/../Contexto Previo GPT/05_template_notebook_fabric.ipynb
para la plantilla original de la que parte esta convención de secciones).

Diferencia importante con esa plantilla original: aquella asumía un único
df_in -> df_out lineal. Este generador anda el grafo completo del IR (con
joins, filters de dos salidas, unions) y crea una variable `df_<ToolID>` por
nodo, en orden topológico — necesario para cualquier workflow con más de un
input o una rama condicional.

Cada bloque de código conserva `# TOOL: <nombre>` y `# SOURCE: ToolID=<id>`
(instrucción de SKILL.md) para que la trazabilidad sobreviva la generación.

En modo `strict`, si algún nodo dispara una condición bloqueante (ver
GenContext.warn con blocking=True — herramienta sin generador, Join
ambiguo, Multi-Row Formula sin orden confirmado, expresión sin transpilar)
el script NO escribe el notebook: imprime la lista de bloqueos y sale con
código 1. En modo `lenient` (default), continúa e inserta `# TODO` ligado
al ToolID, y lo cuenta en el resumen final.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from formula_transpiler import translate_date_format

# --------------------------------------------------------------------------
# SETUP: funciones reutilizables embebidas en el notebook generado.
# Mantenido en sync manualmente con references/spark_patterns.md — si cambias
# una función allá, cambiala aquí también.
# --------------------------------------------------------------------------

SETUP_CODE = '''\
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def read_table(table_fqn: str) -> DataFrame:
    return spark.read.table(table_fqn)

def read_delta_path(path: str) -> DataFrame:
    return spark.read.format('delta').load(path)

def read_csv_path(path: str, header: bool = True, inferSchema: bool = True) -> DataFrame:
    return spark.read.option('header', header).option('inferSchema', inferSchema).csv(path)

def write_delta(df: DataFrame, path: str, mode: str = 'overwrite', partitionBy=None):
    w = df.write.format('delta').mode(mode)
    if partitionBy:
        w = w.partitionBy(*partitionBy)
    w.save(path)

def save_as_table(df: DataFrame, table_fqn: str, mode: str = 'overwrite', partitionBy=None):
    w = df.write.mode(mode)
    if partitionBy:
        w = w.partitionBy(*partitionBy)
    w.saveAsTable(table_fqn)

def rename_right(df_right: DataFrame, keys, prefix: str = 'r_') -> DataFrame:
    exprs = []
    for c in df_right.columns:
        exprs.append(F.col(c) if c in keys else F.col(c).alias(f'{prefix}{c}'))
    return df_right.select(*exprs)

def safe_join(df_left: DataFrame, df_right: DataFrame, keys, how: str = 'inner', right_prefix: str = 'r_') -> DataFrame:
    return df_left.join(rename_right(df_right, keys, prefix=right_prefix), on=keys, how=how)

def group_agg(df: DataFrame, keys, aggs: dict) -> DataFrame:
    agg_exprs = []
    for dst, (fn, field) in aggs.items():
        fn_l = fn.lower()
        if fn_l == 'sum':
            agg_exprs.append(F.sum(field).alias(dst))
        elif fn_l in ('avg', 'mean'):
            agg_exprs.append(F.avg(field).alias(dst))
        elif fn_l == 'min':
            agg_exprs.append(F.min(field).alias(dst))
        elif fn_l == 'max':
            agg_exprs.append(F.max(field).alias(dst))
        elif fn_l == 'count':
            agg_exprs.append(F.count(field).alias(dst))
        elif fn_l in ('countdistinct', 'count_distinct'):
            agg_exprs.append(F.countDistinct(field).alias(dst))
        else:
            raise ValueError(f'Funcion de agregacion no soportada: {fn}')
    return df.groupBy(*keys).agg(*agg_exprs) if keys else df.groupBy().agg(*agg_exprs)

print('[INFO] Notebook generado por alteryx-to-fabric (Fase 4) cargado')\
'''

BLOCKING_HINT_CODES = {
    "ERR_UNSUPPORTED_TOOL", "ERR_MACRO_REQUIRES_DECISION", "ERR_JOIN_OUTPUT_UNDETERMINED",
    "WARN_ORDER_UNDEFINED", "WARN_ROW_REF_WITHOUT_WINDOW", "WARN_PARSE_ERROR",
}


# --------------------------------------------------------------------------
# Grafo: orden topológico y resolución de variables upstream
# --------------------------------------------------------------------------


def topo_sort(ir: dict) -> list[str]:
    node_ids = [n["id"] for n in ir["nodes"]]
    order_index = {nid: i for i, nid in enumerate(node_ids)}
    indeg = {nid: 0 for nid in node_ids}
    adj: dict[str, list[str]] = {nid: [] for nid in node_ids}
    for c in ir["connections"]:
        if c["source_id"] in adj and c["destination_id"] in indeg:
            adj[c["source_id"]].append(c["destination_id"])
            indeg[c["destination_id"]] += 1

    queue = sorted([nid for nid in node_ids if indeg[nid] == 0], key=order_index.get)
    result = []
    while queue:
        queue.sort(key=order_index.get)
        nid = queue.pop(0)
        result.append(nid)
        for nxt in adj[nid]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)

    if len(result) != len(node_ids):
        missing = set(node_ids) - set(result)
        raise ValueError(f"Ciclo detectado o nodos inalcanzables en el grafo: {sorted(missing)}")
    return result


def upstream_var(nodes_by_id: dict, edge: dict) -> str:
    """Nombre de variable que produce el extremo origen de una conexión.
    Filter tiene dos salidas posibles (True/False); el resto, una sola."""
    src = nodes_by_id[edge["source_id"]]
    if src["type"] == "Filter":
        anchor = edge["source_anchor"].strip().lower()
        suffix = "true" if anchor == "true" else "false"
        return f"df_{src['id']}_{suffix}"
    return f"df_{src['id']}"


def first_upstream_var(ir: dict, nodes_by_id: dict, node_id: str) -> str | None:
    edges = [c for c in ir["connections"] if c["destination_id"] == node_id]
    if not edges:
        return None
    return upstream_var(nodes_by_id, edges[0])


def get_sort_order_cols(nodes_by_id: dict, sort_node_id: str) -> list[tuple[str, str]]:
    raw = nodes_by_id[sort_node_id]["params"].get("_raw", {}) or {}
    sort_info = raw.get("SortInfo", {}) or {}
    fields = sort_info.get("Field", [])
    if isinstance(fields, dict):
        fields = [fields]
    out = []
    for f in fields:
        name = f.get("@field")
        if name:
            out.append((name, f.get("@order", "Ascending")))
    return out


# --------------------------------------------------------------------------
# Inferencia del tipo de Join a partir de las anclas realmente conectadas
# aguas abajo (Left/Right/Join — ver la nota de compatibilidad en
# references/tool_mapping.md sobre por qué NO son "J/L/R").
# --------------------------------------------------------------------------


def infer_join_how(ir: dict, join_id: str) -> tuple[str | None, str, list[str]]:
    outs = sorted({c["source_anchor"] for c in ir["connections"] if c["source_id"] == join_id})
    outs_set = set(outs)
    if outs_set <= {"Join"}:
        return "inner", "explicit_join_only", outs
    if outs_set == {"Join", "Left"}:
        return "left", "explicit_join_left", outs
    if outs_set == {"Join", "Right"}:
        return "right", "explicit_join_right", outs
    if outs_set == {"Join", "Left", "Right"}:
        return "outer", "explicit_join_left_right", outs
    if outs_set == {"Left"}:
        return "left_anti", "left_only_anti", outs
    if outs_set == {"Right"}:
        return None, "right_only_anti_unsupported", outs
    return None, "ambiguous", outs


# --------------------------------------------------------------------------
# Contexto de generación: acumula warnings y decide qué bloquea en strict
# --------------------------------------------------------------------------


class GenContext:
    def __init__(self, mode: str):
        self.mode = mode
        self.warnings: list[dict] = []
        self.blocking: list[dict] = []
        self.todo_count = 0

    def warn(self, node: dict, code: str, message: str, blocking: bool = False) -> None:
        entry = {"code": code, "tool_id": node["id"], "tool": node["type"], "message": message}
        self.warnings.append(entry)
        if blocking or code in BLOCKING_HINT_CODES:
            self.todo_count += 1
            if self.mode == "strict":
                self.blocking.append(entry)


# --------------------------------------------------------------------------
# Codegen por herramienta
# --------------------------------------------------------------------------


def cg_input_data(node: dict, ctx: GenContext, **_) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    p = node["params"]
    fmt, path = p.get("format", "unknown"), p.get("file_path", "")
    if fmt == "csv":
        return [f"{var} = read_csv_path({path!r})"], var
    if fmt == "delta":
        return [f"{var} = read_delta_path({path!r})"], var
    if fmt == "excel":
        ctx.warn(node, "WARN_UNSUPPORTED_INPUT_FORMAT", f"Excel sin lector nativo de Spark ({path!r}).", blocking=True)
        return [f"{var} = None  # TODO: leer Excel a mano -- pandas.read_excel({path!r}) + spark.createDataFrame(...) (ver references/tool_mapping.md)"], var
    if fmt == "yxdb":
        ctx.warn(node, "WARN_UNSUPPORTED_INPUT_FORMAT", f"'.yxdb' es formato binario propietario de Alteryx, sin lector en Spark ({path!r}).", blocking=True)
        return [f"{var} = None  # TODO: '{path}' es .yxdb -- exportar desde Alteryx a Parquet/CSV/Delta antes de poder leerlo aqui"], var
    ctx.warn(node, "WARN_UNSUPPORTED_INPUT_FORMAT", f"Formato de entrada {fmt!r} no reconocido ({path!r}).", blocking=True)
    return [f"{var} = None  # TODO: formato {fmt!r} no reconocido (ToolID={node['id']}), revisar manualmente"], var


def cg_select(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    p = node["params"]
    lines = [f"{var} = {upstream}"]
    keep = p.get("keep_fields", [])
    if keep:
        lines.append(f"{var} = {var}.select({', '.join(repr(c) for c in keep)})")
    for old, new in p.get("rename", {}).items():
        lines.append(f"{var} = {var}.withColumnRenamed({old!r}, {new!r})")
    return lines, var


def cg_filter(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], None]:
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    code = node["params"].get("pyspark_code")
    var_true, var_false = f"df_{node['id']}_true", f"df_{node['id']}_false"
    out_anchors = {c["source_anchor"] for c in ir["connections"] if c["source_id"] == node["id"]}

    if not code:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Filter sin pyspark_code — corre transpile_formula.py antes de generar el notebook.", blocking=True)
        return [f"{var_true} = {upstream}  # TODO: Filter ToolID={node['id']} sin transpilar",
                f"{var_false} = {upstream}  # TODO: Filter ToolID={node['id']} sin transpilar"], None

    lines = []
    if "True" in out_anchors or not out_anchors:
        lines.append(f"{var_true} = {upstream}.filter({code})")
    if "False" in out_anchors:
        lines.append(f"{var_false} = {upstream}.filter((~({code})) | (({code}).isNull()))")
    return lines, None


def cg_formula(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    exprs = node["params"].get("expressions", [])
    if not exprs:
        blocking = node["type"] != "Formula"  # Multi-Field Formula no tiene extractor en la Fase 2 todavia
        code = "ERR_UNSUPPORTED_TOOL" if blocking else "WARN_UNSUPPORTED_EXPRESSION"
        ctx.warn(node, code, f"{node['type']} sin expresiones estructuradas; revisar params._raw manualmente.", blocking=blocking)
        return [f"{var} = {upstream}  # TODO: {node['type']} ToolID={node['id']} no traspilado, revisar params._raw"], var

    lines = [f"{var} = {upstream}"]
    for e in exprs:
        code = e.get("pyspark_code")
        if code:
            lines.append(f"{var} = {var}.withColumn({e['as']!r}, {code})")
        else:
            ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", f"Campo {e.get('as')!r} sin pyspark_code.", blocking=True)
            lines.append(f"# TODO: campo {e.get('as')!r} sin transpilar (ToolID={node['id']})")
    return lines, var


def build_window_code(node: dict, ctx: GenContext, nodes_by_id: dict, partition_cols: list[str]) -> str | None:
    upstream_ids = node.get("inputs", [])
    upstream_type = nodes_by_id[upstream_ids[0]]["type"] if upstream_ids and upstream_ids[0] in nodes_by_id else None

    if upstream_type == "Sort":
        cols = get_sort_order_cols(nodes_by_id, upstream_ids[0])
        if cols:
            order_code = ", ".join(
                f"F.col({name!r}).desc()" if order.lower().startswith("desc") else f"F.col({name!r}).asc()"
                for name, order in cols
            )
        else:
            order_code = None
    else:
        order_code = None

    if order_code is None:
        ctx.warn(
            node, "WARN_ORDER_UNDEFINED",
            "No hay Sort explicito aguas arriba (o no se pudieron leer sus columnas); "
            "el orden de Spark no es deterministico para este nodo.",
            blocking=True,
        )
        if ctx.mode == "strict":
            return None
        order_code = "F.monotonically_increasing_id()"

    partition_code = ", ".join(repr(c) for c in partition_cols)
    return f"Window.partitionBy({partition_code}).orderBy({order_code})"


def cg_multi_row_formula(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    p = node["params"]
    window_var = p.get("window_var", f"w_{node['id']}")

    window_code = build_window_code(node, ctx, nodes_by_id, p.get("group_by", []))
    if window_code is None:
        return [f"{var} = {upstream}  # TODO: confirmar columna de orden para Multi-Row Formula ToolID={node['id']} (modo strict)"], var

    lines = [f"{window_var} = {window_code}"]
    code = p.get("pyspark_code")
    field = p.get("update_field") or f"campo_{node['id']}"
    if code:
        lines.append(f"{var} = {upstream}.withColumn({field!r}, {code})")
    else:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Multi-Row Formula sin pyspark_code.", blocking=True)
        lines.append(f"{var} = {upstream}  # TODO: falta transpilar expresion (ToolID={node['id']})")
    return lines, var


def cg_join(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    incoming = [c for c in ir["connections"] if c["destination_id"] == node["id"]]
    left_edge = next((c for c in incoming if c["destination_anchor"] == "Left"), None)
    right_edge = next((c for c in incoming if c["destination_anchor"] == "Right"), None)

    if left_edge is None or right_edge is None:
        ctx.warn(node, "ERR_JOIN_OUTPUT_UNDETERMINED", "No se encontraron ambas entradas Left/Right conectadas.", blocking=True)
        return [f"{var} = None  # TODO: Join ToolID={node['id']} sin ambas entradas Left/Right resueltas"], var

    left_var, right_var = upstream_var(nodes_by_id, left_edge), upstream_var(nodes_by_id, right_edge)
    keys = node["params"].get("join_keys", {"left": [], "right": []})
    left_keys, right_keys = keys.get("left", []), keys.get("right", [])

    how, rule, outs = infer_join_how(ir, node["id"])
    lines = []
    if how is None:
        blocking = rule == "ambiguous"
        code = "ERR_JOIN_OUTPUT_UNDETERMINED" if blocking else "WARN_JOIN_RIGHT_ANTI_UNSUPPORTED"
        ctx.warn(node, code, f"No se pudo inferir 'how' automaticamente (salidas conectadas={outs}, regla={rule}).", blocking=blocking)
        how = "inner"
        lines.append(f"# DECISION: how='inner' -- FALLBACK, revisar manualmente (regla={rule}, salidas={outs})")
    else:
        ctx.warn(node, "DECISION_JOIN_MODE_INFERRED", f"how={how!r} inferido (regla={rule}, salidas conectadas={outs}).")
        lines.append(f"# DECISION: how={how!r} (regla={rule}, salidas conectadas={outs})")

    if not (left_keys and right_keys and len(left_keys) == len(right_keys)):
        ctx.warn(node, "WARN_JOIN_KEYS_INCOMPLETE", "Llaves de join incompletas o asimetricas.", blocking=True)
        lines.append(f"{var} = {left_var}  # TODO: Join ToolID={node['id']} sin llaves resueltas, completar a mano")
        return lines, var

    if left_keys == right_keys:
        lines.append(f"{var} = safe_join({left_var}, {right_var}, {left_keys!r}, how={how!r})")
    else:
        # Llaves con nombre distinto por lado: renombrar la llave derecha antes del join
        # (safe_join/rename_right asumen mismo nombre en ambos lados -- ver spark_patterns.md).
        tmp = f"_right_{node['id']}"
        lines.append(f"{tmp} = rename_right({right_var}, {right_keys!r}, prefix='r_')")
        for l, r in zip(left_keys, right_keys):
            lines.append(f"{tmp} = {tmp}.withColumnRenamed({r!r}, {l!r})")
        lines.append(f"{var} = {left_var}.join({tmp}, on={left_keys!r}, how={how!r})")

    return lines, var


def cg_union(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    incoming = [c for c in ir["connections"] if c["destination_id"] == node["id"]]
    seen, ordered_vars = set(), []
    for edge in incoming:
        v = upstream_var(nodes_by_id, edge)
        if v not in seen:
            seen.add(v)
            ordered_vars.append(v)

    if len(ordered_vars) < len(incoming):
        ctx.warn(
            node, "WARN_UNION_AMBIGUO_POST_JOIN",
            f"Union recibe {len(incoming)} conexiones pero solo {len(ordered_vars)} DataFrames distintos "
            "(probable reconstruccion de ramas de un Join aguas arriba); se genero sin duplicar filas. "
            "Verificar el 'how' inferido en ese Join.",
        )

    if not ordered_vars:
        ctx.warn(node, "ERR_UNSUPPORTED_TOOL", "Union sin entradas resueltas.", blocking=True)
        return [f"{var} = None  # TODO: Union ToolID={node['id']} sin entradas resueltas"], var

    lines = [f"{var} = {ordered_vars[0]}"]
    for v in ordered_vars[1:]:
        lines.append(f"{var} = {var}.unionByName({v}, allowMissingColumns=True)")
    return lines, var


def cg_summarize(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    p = node["params"]
    group_by, aggs = p.get("group_by", []), p.get("aggregations", [])
    if not aggs:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Summarize sin aggregations resueltas.", blocking=True)
        return [f"{var} = {upstream}  # TODO: Summarize ToolID={node['id']} sin config resuelta"], var
    aggs_dict = "{" + ", ".join(f"{a['as']!r}: ({a['op']!r}, {a['field']!r})" for a in aggs) + "}"
    return [f"{var} = group_agg({upstream}, {group_by!r}, {aggs_dict})"], var


def cg_sort(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    cols = get_sort_order_cols(nodes_by_id, node["id"])
    if not cols:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Sort sin columnas resueltas en _raw.SortInfo.", blocking=True)
        return [f"{var} = {upstream}  # TODO: Sort ToolID={node['id']} sin columnas resueltas"], var
    order_code = ", ".join(
        f"F.col({name!r}).desc()" if order.lower().startswith("desc") else f"F.col({name!r}).asc()"
        for name, order in cols
    )
    return [f"{var} = {upstream}.orderBy({order_code})"], var


def cg_unique(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    raw = node["params"].get("_raw", {}) or {}
    fields = (raw.get("UniqueFields") or {}).get("Field", [])
    if isinstance(fields, dict):
        fields = [fields]
    cols = [f.get("@field") for f in fields if f.get("@field")]
    if not cols:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Unique sin columnas resueltas en _raw.UniqueFields.", blocking=True)
        return [f"{var} = {upstream}  # TODO: Unique ToolID={node['id']} sin columnas resueltas"], var
    return [f"{var} = {upstream}.dropDuplicates({cols!r})"], var


def cg_text_to_columns(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    raw = node["params"].get("_raw", {}) or {}
    field = raw.get("Field")
    root_name = raw.get("RootName") or field
    delims = (raw.get("Delimeters") or {}).get("@value", "")
    try:
        num_fields = int((raw.get("NumFields") or {}).get("@value"))
    except (TypeError, ValueError):
        num_fields = None
    error_handling = raw.get("ErrorHandling", "")

    if not field or not num_fields:
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "Text To Columns sin config resuelta en _raw.", blocking=True)
        return [f"{var} = {upstream}  # TODO: Text To Columns ToolID={node['id']} sin config resuelta"], var
    if error_handling and error_handling != "Last":
        ctx.warn(
            node, "WARN_UNSUPPORTED_EXPRESSION",
            f"Text To Columns con ErrorHandling={error_handling!r} no verificado contra un caso real "
            "(se genero como si fuera 'Last': el remanente queda en el ultimo campo).",
        )

    pattern = "[" + re.escape(delims) + "]" if delims else ","
    tmp = f"_split_{node['id']}"
    lines = [f"{var} = {upstream}.withColumn({tmp!r}, F.split(F.col({field!r}), {pattern!r}, {num_fields}))"]
    for i in range(num_fields):
        lines.append(f"{var} = {var}.withColumn({f'{root_name}{i + 1}'!r}, F.col({tmp!r}).getItem({i}))")
    lines.append(f"{var} = {var}.drop({tmp!r})")
    return lines, var


def cg_datetime_tool(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    """Herramienta dedicada 'DateTime' (distinta de las funciones DateTimeParse/
    DateTimeFormat de Formula). Usa formato bare java.time (`yyyy-MM-dd`), NO
    strftime -- confirmado contra un caso real; translate_date_format() es un
    no-op sobre ese formato asi que aplicarla de todas formas no hace dano."""
    var = f"df_{node['id']}"
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    raw = node["params"].get("_raw", {}) or {}
    in_field, out_field, fmt = raw.get("InputFieldName"), raw.get("OutputFieldName"), raw.get("Format")
    is_from = (raw.get("IsFrom") or {}).get("@value") == "True"

    if not (in_field and out_field and fmt):
        ctx.warn(node, "WARN_UNSUPPORTED_EXPRESSION", "DateTime (herramienta) sin config resuelta en _raw.", blocking=True)
        return [f"{var} = {upstream}  # TODO: DateTime ToolID={node['id']} sin config resuelta"], var

    spark_fmt, unknown = translate_date_format(fmt)
    if unknown:
        ctx.warn(node, "WARN_UNSUPPORTED_DATE_TOKEN", f"Token(s) sin mapeo conocido {unknown} en formato {fmt!r}.")

    if is_from:
        expr = f"F.date_format(F.col({in_field!r}), {spark_fmt!r})"
    else:
        has_time = any(tok in fmt for tok in ("H", "h", ":", "s", "S"))
        fn = "F.to_timestamp" if has_time else "F.to_date"
        expr = f"{fn}(F.col({in_field!r}), {spark_fmt!r})"

    return [f"{var} = {upstream}.withColumn({out_field!r}, {expr})"], var


def cg_output_data(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], None]:
    upstream = first_upstream_var(ir, nodes_by_id, node["id"]) or "None"
    p = node["params"]
    fmt, target = p.get("format", "delta"), p.get("target", "")
    if fmt == "table":
        return [f"save_as_table({upstream}, {target!r})"], None
    if fmt != "delta":
        ctx.warn(node, "WARN_UNSUPPORTED_OUTPUT_FORMAT", f"Formato de salida {fmt!r} no reconocido; se genero como delta.")
    return [f"write_delta({upstream}, {target!r})"], None


def cg_fallback(node: dict, ctx: GenContext, ir: dict, nodes_by_id: dict) -> tuple[list[str], str]:
    var = f"df_{node['id']}"
    upstream_ids = node.get("inputs", [])
    upstream = f"df_{upstream_ids[0]}" if upstream_ids else "None"
    code = "ERR_MACRO_REQUIRES_DECISION" if node["type"] == "Macro" else "ERR_UNSUPPORTED_TOOL"
    ctx.warn(node, code, f"{node['type']} sin generador automatico; ver references/tool_mapping.md.", blocking=True)
    return [f"{var} = {upstream}  # TODO: {node['type']} ToolID={node['id']} sin traducir, ver references/tool_mapping.md"], var


TRANSFORM_HANDLERS = {
    "Select": cg_select,
    "Filter": cg_filter,
    "Formula": cg_formula,
    "Multi-Field Formula": cg_formula,
    "Multi-Row Formula": cg_multi_row_formula,
    "Join": cg_join,
    "Union": cg_union,
    "Summarize": cg_summarize,
    "Sort": cg_sort,
    "Unique": cg_unique,
    "Text To Columns": cg_text_to_columns,
    "DateTime": cg_datetime_tool,
}


# --------------------------------------------------------------------------
# Ensamblado del notebook
# --------------------------------------------------------------------------


def generate_blocks(ir: dict, mode: str) -> tuple[list, list, list, list, GenContext]:
    nodes_by_id = {n["id"]: n for n in ir["nodes"]}
    ctx = GenContext(mode)
    read_blocks, transform_blocks, output_blocks, qa_blocks = [], [], [], []

    for nid in topo_sort(ir):
        node = nodes_by_id[nid]
        ttype = node["type"]
        header = [f"# TOOL: {ttype}", f"# SOURCE: ToolID={nid}"]

        if ttype == "Input Data":
            lines, _ = cg_input_data(node, ctx)
            read_blocks.append(header + lines)
        elif ttype == "Output Data":
            lines, _ = cg_output_data(node, ctx, ir, nodes_by_id)
            output_blocks.append(header + lines)
            upstream_id = node["inputs"][0] if node.get("inputs") else None
            if upstream_id:
                qa_blocks.append(header + [f"print('[QA] ToolID={nid} filas_en_salida:', df_{upstream_id}.count())"])
        else:
            handler = TRANSFORM_HANDLERS.get(ttype, cg_fallback)
            lines, _ = handler(node, ctx, ir, nodes_by_id)
            transform_blocks.append(header + lines)

    return read_blocks, transform_blocks, output_blocks, qa_blocks, ctx


def _split_source(text: str) -> list[str]:
    lines = text.splitlines()
    if not lines:
        return []
    return [ln + "\n" for ln in lines[:-1]] + [lines[-1]]


def make_code_cell(text: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": _split_source(text)}


def make_markdown_cell(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": _split_source(text)}


def build_notebook(ir: dict, mode: str, lakehouse_name: str) -> tuple[dict | None, GenContext]:
    read_blocks, transform_blocks, output_blocks, qa_blocks, ctx = generate_blocks(ir, mode)

    if mode == "strict" and ctx.blocking:
        return None, ctx

    wf_name = ir.get("workflow", {}).get("name", "workflow")
    cells = [
        make_markdown_cell(f"# {wf_name}\nGenerado por `alteryx-to-fabric` (Fase 4). Lakehouse: `{lakehouse_name}`."),
        make_markdown_cell("## 1) SETUP"),
        make_code_cell(SETUP_CODE),
        make_markdown_cell("## 2) READ"),
    ]
    cells += [make_code_cell("\n".join(b)) for b in read_blocks] or [make_code_cell("# (sin nodos Input Data)")]

    cells.append(make_markdown_cell("## 3) TRANSFORMS"))
    cells += [make_code_cell("\n".join(b)) for b in transform_blocks] or [make_code_cell("# (sin transformaciones)")]

    cells.append(make_markdown_cell("## 4) QA"))
    cells += [make_code_cell("\n".join(b)) for b in qa_blocks] or [make_code_cell("# (sin nodos Output Data)")]

    cells.append(make_markdown_cell("## 5) OUTPUTS"))
    cells += [make_code_cell("\n".join(b)) for b in output_blocks] or [make_code_cell("# (sin nodos Output Data)")]

    warn_counts: dict[str, int] = {}
    for w in ctx.warnings:
        warn_counts[w["code"]] = warn_counts.get(w["code"], 0) + 1

    logging_code = (
        "import json\n"
        "payload = {\n"
        f"    'workflow': {wf_name!r},\n"
        f"    'modo': {mode!r},\n"
        f"    'todos_pendientes': {ctx.todo_count},\n"
        f"    'warnings_por_codigo': {warn_counts!r},\n"
        "}\n"
        "print('[EXIT]', json.dumps(payload, ensure_ascii=False))"
    )
    cells.append(make_markdown_cell("## 6) LOGGING"))
    cells.append(make_code_cell(logging_code))

    notebook = {
        "cells": cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return notebook, ctx


def main() -> int:
    ap = argparse.ArgumentParser(description="Genera un notebook PySpark/Fabric a partir del IR transpilado")
    ap.add_argument("path", type=Path, help="IR JSON transpilado (salida de transpile_formula.py)")
    ap.add_argument("--out", type=Path, default=None, help="Archivo .ipynb de salida")
    ap.add_argument("--modo", choices=["strict", "lenient"], default="lenient")
    ap.add_argument("--lakehouse-name", default="Lakehouse")
    args = ap.parse_args()

    if not args.path.exists():
        print(f"No existe: {args.path}", file=sys.stderr)
        return 1

    ir = json.loads(args.path.read_text(encoding="utf-8"))
    notebook, ctx = build_notebook(ir, args.modo, args.lakehouse_name)

    if notebook is None:
        print(f"BLOQUEADO en modo strict — {len(ctx.blocking)} problema(s) sin resolver:", file=sys.stderr)
        for b in ctx.blocking:
            print(f"  [{b['code']}] ToolID={b['tool_id']} ({b['tool']}): {b['message']}", file=sys.stderr)
        print("Resuelve estos puntos o vuelve a correr en --modo lenient para obtener un notebook con TODOs.", file=sys.stderr)
        return 1

    out_path = args.out or args.path.with_suffix(".ipynb")
    out_path.write_text(json.dumps(notebook, indent=1, ensure_ascii=False), encoding="utf-8")

    for w in ctx.warnings:
        print(f"  [{w['code']}] ToolID={w['tool_id']} ({w['tool']}): {w['message']}")
    print(f"OK  notebook generado -> {out_path} ({ctx.todo_count} TODO(s) pendientes, modo={args.modo})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
