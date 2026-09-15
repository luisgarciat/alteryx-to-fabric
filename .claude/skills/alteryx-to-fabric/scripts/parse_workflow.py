#!/usr/bin/env python3
"""
Parser de workflows Alteryx (.yxmd/.yxmc/.yxzp) al modelo intermedio JSON
descrito en references/../01_modelo_intermedio_schema.json (paquete GPT
original) y ejemplificado en references/tool_mapping.md.

Determinista: el mismo archivo produce siempre el mismo JSON (salvo
`metadata.extracted_at`, que es la hora de la corrida).

Uso:
    python scripts/parse_workflow.py flujo.yxmd
    python scripts/parse_workflow.py flujo.yxzp --out ./ir/flujo.ir.json
    python scripts/parse_workflow.py /ruta/a/workflows --out ./ir

Sin dependencias externas: solo stdlib. Reutiliza la tabla de alias de
herramientas y el descubrimiento de archivos de scripts/inventory.py — si
inventory.py aprende un alias nuevo, este parser lo hereda automáticamente.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from inventory import COSMETIC_TOOLS, basename, iter_workflow_sources, normalize_plugin

PARSER_VERSION = "v0.1.0"

# Herramientas donde el orden de registro importa. Si ninguna de sus entradas
# directas es un Sort explícito, el resultado no es determinista en Spark.
# Ver references/warnings.md -> WARN_ORDER_UNDEFINED.
ORDER_SENSITIVE_TOOLS = {"Multi-Row Formula", "Running Total", "Record ID"}


# --------------------------------------------------------------------------
# Utilidades XML
# --------------------------------------------------------------------------


def text_of(el: ET.Element | None) -> str:
    return (el.text or "").strip() if el is not None else ""


def xml_to_dict(el: ET.Element) -> Any:
    """Conversión genérica de un elemento XML a dict/list/str, para no perder
    ninguna configuración de herramientas que todavía no tienen extractor
    específico. Se guarda como params['_raw']."""
    node: dict[str, Any] = {f"@{k}": v for k, v in el.attrib.items()}
    children = list(el)
    if not children:
        text = (el.text or "").strip()
        if text:
            if node:
                node["#text"] = text
                return node
            return text
        return node or None

    child_map: dict[str, list[Any]] = defaultdict(list)
    for child in children:
        child_map[child.tag].append(xml_to_dict(child))
    for tag, values in child_map.items():
        node[tag] = values[0] if len(values) == 1 else values
    return node


def guess_format(path: str) -> str:
    ext = Path(path.split("|")[0]).suffix.lower().lstrip(".")
    return {"csv": "csv", "txt": "csv", "xlsx": "excel", "xls": "excel", "yxdb": "yxdb"}.get(
        ext, ext or "unknown"
    )


# --------------------------------------------------------------------------
# Extractores de params por herramienta (las de mayor frecuencia según el
# inventario). Todo lo demás cae al fallback genérico `_raw`.
# --------------------------------------------------------------------------


def extract_input_data(cfg: ET.Element | None) -> dict:
    raw_path = text_of(cfg.find("File")) if cfg is not None else ""
    file_path = raw_path.split("|")[0]
    return {"file_path": file_path, "format": guess_format(file_path)}


def extract_output_data(cfg: ET.Element | None) -> dict:
    raw_path = text_of(cfg.find("File")) if cfg is not None else ""
    target = raw_path.split("|")[0]
    return {"target": target, "format": guess_format(target)}


def extract_select(cfg: ET.Element | None) -> dict:
    keep, drop, rename = [], [], {}
    sf = cfg.find("SelectFields") if cfg is not None else None
    if sf is not None:
        for f in sf.findall("SelectField"):
            field = f.get("field", "")
            if not field or field == "*Unknown":
                continue
            if f.get("selected", "True") == "True":
                keep.append(field)
                new_name = f.get("rename", "")
                if new_name and new_name != field:
                    rename[field] = new_name
            else:
                drop.append(field)
    out: dict[str, Any] = {"keep_fields": keep}
    if drop:
        out["drop_fields"] = drop
    if rename:
        out["rename"] = rename
    return out


def extract_filter(cfg: ET.Element | None) -> dict:
    if cfg is None:
        return {}
    return {"expression": text_of(cfg.find("Expression")), "mode": text_of(cfg.find("Mode")) or "Custom"}


def extract_formula(cfg: ET.Element | None) -> dict:
    exprs = []
    ff = cfg.find("FormulaFields") if cfg is not None else None
    if ff is not None:
        for f in ff.findall("FormulaField"):
            exprs.append({"as": f.get("field", ""), "expr": f.get("expression", "")})
    return {"expressions": exprs}


def extract_multi_row_formula(cfg: ET.Element | None) -> dict:
    if cfg is None:
        return {}
    group_by = []
    gbf = cfg.find("GroupByFields")
    if gbf is not None:
        group_by = [f.get("field", "") for f in gbf.findall("Field") if f.get("field")]
    return {
        "expression": text_of(cfg.find("Expression")),
        "update_field": text_of(cfg.find("UpdateFieldName")),
        "group_by": group_by,
    }


def extract_join(cfg: ET.Element | None) -> dict:
    keys: dict[str, list[str]] = {"left": [], "right": []}
    if cfg is not None:
        for ji in cfg.findall("JoinInfo"):
            side = ji.get("connection", "").lower()
            if side not in keys:
                continue
            keys[side] = [f.get("field", "") for f in ji.findall("Field") if f.get("field")]
    # join_type NO se determina aquí a propósito: depende de qué salidas
    # (J/L/R) estén conectadas aguas abajo, y eso es resorte del paso de
    # inferencia (join_inference.py adaptado), no del parser. Ver tool_mapping.md.
    return {"join_keys": keys}


def extract_union(cfg: ET.Element | None) -> dict:
    mode = "auto"
    if cfg is not None:
        m = text_of(cfg.find("MergeMode"))
        if m:
            mode = m
    return {"merge_mode": mode}


def extract_summarize(cfg: ET.Element | None) -> dict:
    group_by, aggs = [], []
    sf = cfg.find("SummarizeFields") if cfg is not None else None
    if sf is not None:
        for f in sf.findall("SummarizeField"):
            field = f.get("field", "")
            action = f.get("action", "")
            rename = f.get("rename", field)
            if action.lower() == "groupby":
                group_by.append(field)
            else:
                aggs.append({"op": action.lower(), "field": field, "as": rename})
    return {"group_by": group_by, "aggregations": aggs}


EXTRACTORS = {
    "Input Data": extract_input_data,
    "Output Data": extract_output_data,
    "Select": extract_select,
    "Filter": extract_filter,
    "Formula": extract_formula,
    "Multi-Row Formula": extract_multi_row_formula,
    "Join": extract_join,
    "Union": extract_union,
    "Summarize": extract_summarize,
}


def build_params(tool_type: str, node_el: ET.Element, extra: dict | None = None) -> dict:
    props = node_el.find("Properties")
    cfg = props.find("Configuration") if props is not None else None

    params = dict(extra) if extra else {}
    extractor = EXTRACTORS.get(tool_type)
    if extractor:
        params.update(extractor(cfg))

    if cfg is not None:
        raw = xml_to_dict(cfg)
        if raw:
            params["_raw"] = raw
    return params


# --------------------------------------------------------------------------
# Recorrido del grafo
# --------------------------------------------------------------------------


def collect_nodes(nodes_el: ET.Element, out: dict[str, dict]) -> None:
    """Recorre <Nodes>, entra en <ChildNodes> de los contenedores (que no se
    emiten como nodos propios: solo agrupan visualmente) y descarta el resto
    de herramientas cosméticas."""
    for node_el in nodes_el.findall("Node"):
        tool_id = node_el.get("ToolID", "")
        gui = node_el.find("GuiSettings")
        plugin = gui.get("Plugin", "") if gui is not None else ""
        engine = node_el.find("EngineSettings")
        macro = engine.get("Macro", "") if engine is not None else ""

        tool_type = "Macro" if macro else normalize_plugin(plugin)

        if tool_type not in COSMETIC_TOOLS:
            extra = {"macro_path": macro} if macro else None
            params = build_params(tool_type, node_el, extra=extra)
            out[tool_id] = {"id": tool_id, "type": tool_type, "inputs": [], "outputs": [], "params": params}

        child = node_el.find("ChildNodes")
        if child is not None:
            collect_nodes(child, out)


def append_unique(lst: list[str], val: str) -> None:
    if val not in lst:
        lst.append(val)


def collect_connections(root: ET.Element, known_ids: set[str]) -> list[dict]:
    conns_el = root.find("Connections")
    result = []
    if conns_el is None:
        return result
    for conn in conns_el.findall("Connection"):
        origin = conn.find("Origin")
        dest = conn.find("Destination")
        if origin is None or dest is None:
            continue
        src_id, dst_id = origin.get("ToolID", ""), dest.get("ToolID", "")
        if src_id not in known_ids or dst_id not in known_ids:
            continue  # uno de los extremos es cosmético/contenedor: se descarta
        result.append(
            {
                "source_id": src_id,
                "source_anchor": origin.get("Connection", "Output"),
                "destination_id": dst_id,
                "destination_anchor": dest.get("Connection", "Input"),
            }
        )
    return result


def check_order_warnings(nodes: dict[str, dict]) -> list[dict]:
    warnings = []
    for nid, node in nodes.items():
        if node["type"] not in ORDER_SENSITIVE_TOOLS:
            continue
        upstream_types = {nodes[u]["type"] for u in node["inputs"] if u in nodes}
        if "Sort" not in upstream_types:
            warnings.append(
                {
                    "code": "WARN_ORDER_UNDEFINED",
                    "tool_id": nid,
                    "tool": node["type"],
                    "message": (
                        f"{node['type']} (ToolID={nid}) no tiene un Sort explícito aguas arriba. "
                        "Confirma la columna de orden antes de generar código determinista."
                    ),
                }
            )
    return warnings


def workflow_metadata(root: ET.Element, name: str) -> dict:
    version = root.get("yxmdVer", "unknown")
    description = ""
    props = root.find("Properties")
    if props is not None:
        meta = props.find("MetaInfo")
        if meta is not None:
            description = text_of(meta.find("Description"))
    return {"name": name, "version": version, "description": description}


# --------------------------------------------------------------------------
# Entry point de parseo (una fuente -> un IR)
# --------------------------------------------------------------------------


def parse_workflow_to_ir(label: str, raw: bytes, user: str) -> dict:
    root = ET.fromstring(raw)
    name = Path(label.split("!")[0]).stem

    nodes: dict[str, dict] = {}
    nodes_el = root.find("Nodes")
    if nodes_el is not None:
        collect_nodes(nodes_el, nodes)

    connections = collect_connections(root, set(nodes.keys()))
    for c in connections:
        append_unique(nodes[c["destination_id"]]["inputs"], c["source_id"])
        append_unique(nodes[c["source_id"]]["outputs"], c["destination_id"])

    def sort_key(nid: str) -> tuple:
        return (0, int(nid)) if nid.isdigit() else (1, nid)

    node_list = [nodes[nid] for nid in sorted(nodes, key=sort_key)]
    warnings = check_order_warnings(nodes)

    return {
        "workflow": workflow_metadata(root, name),
        "nodes": node_list,
        "connections": connections,
        "metadata": {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "parser_version": PARSER_VERSION,
            "user": user,
            "warnings": warnings,
        },
    }


# --------------------------------------------------------------------------
# Descubrimiento de archivos (archivo único, o carpeta vía inventory.py)
# --------------------------------------------------------------------------


def load_single_path_sources(path: Path):
    suffix = path.suffix.lower()
    if suffix in (".yxmd", ".yxmc"):
        yield str(path), path.read_bytes()
    elif suffix == ".yxzp":
        with zipfile.ZipFile(path) as zf:
            for member in zf.namelist():
                if member.lower().endswith((".yxmd", ".yxmc")):
                    yield f"{path}!{member}", zf.read(member)
    else:
        raise ValueError(f"Extensión no soportada: {suffix} (usa .yxmd, .yxmc o .yxzp)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Parser de workflows Alteryx a modelo intermedio JSON")
    ap.add_argument("path", type=Path, help="Archivo .yxmd/.yxmc/.yxzp o carpeta con varios")
    ap.add_argument("--out", type=Path, default=None, help="Archivo o carpeta de salida")
    ap.add_argument("--user", default=os.environ.get("USERNAME") or os.environ.get("USER") or "unknown")
    args = ap.parse_args()

    if not args.path.exists():
        print(f"No existe: {args.path}", file=sys.stderr)
        return 1

    if args.path.is_dir():
        sources = list(iter_workflow_sources(args.path))
    else:
        try:
            sources = list(load_single_path_sources(args.path))
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    if not sources:
        print("No se encontraron workflows .yxmd/.yxmc/.yxzp", file=sys.stderr)
        return 1

    batch = args.path.is_dir() or len(sources) > 1
    total_warnings = 0
    converted = 0

    if batch:
        out_dir = args.out or Path("./ir_output")
        out_dir.mkdir(parents=True, exist_ok=True)
        for label, raw in sources:
            if not raw:
                print(f"  [error] vacío/ilegible: {label}", file=sys.stderr)
                continue
            try:
                ir = parse_workflow_to_ir(label, raw, args.user)
            except ET.ParseError as exc:
                print(f"  [error] {label}: {exc}", file=sys.stderr)
                continue
            out_path = out_dir / f"{Path(label.split('!')[0]).stem}.ir.json"
            out_path.write_text(json.dumps(ir, indent=2, ensure_ascii=False), encoding="utf-8")
            converted += 1
            for w in ir["metadata"]["warnings"]:
                print(f"  [{w['code']}] {label}: {w['message']}")
                total_warnings += 1
        print(f"OK  {converted}/{len(sources)} workflows convertidos a IR en {out_dir}/ ({total_warnings} advertencias)")
        return 0

    # Un solo workflow -> un solo archivo de salida
    label, raw = sources[0]
    if not raw:
        print(f"[error] vacío/ilegible: {label}", file=sys.stderr)
        return 1
    try:
        ir = parse_workflow_to_ir(label, raw, args.user)
    except ET.ParseError as exc:
        print(f"[error] {label}: {exc}", file=sys.stderr)
        return 1

    out_path = args.out or args.path.with_suffix(".ir.json")
    out_path.write_text(json.dumps(ir, indent=2, ensure_ascii=False), encoding="utf-8")
    for w in ir["metadata"]["warnings"]:
        print(f"[{w['code']}] {w['message']}")
        total_warnings += 1
    print(f"OK  {len(ir['nodes'])} nodos, {len(ir['connections'])} conexiones -> {out_path} ({total_warnings} advertencias)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
