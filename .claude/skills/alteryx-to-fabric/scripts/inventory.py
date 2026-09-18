#!/usr/bin/env python3
"""
Inventario de workflows Alteryx.

Recorre un directorio de archivos .yxmd / .yxzp / .yxmc y responde:
  - Qué herramientas usan REALMENTE tus flujos, y con qué frecuencia.
  - Cuántos workflows quedarían cubiertos si soportamos las top-N herramientas.
  - Qué macros custom existen (el caso duro de la migración).
  - Cuántas expresiones de Formula hay que transpilar.
  - Qué workflow conviene usar como piloto (complejidad mediana).

Uso:
    python scripts/inventory.py /ruta/a/workflows --out ./inventory
    python scripts/inventory.py /ruta/a/workflows --out ./inventory --top 25

Sin dependencias externas: solo stdlib.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from xml.etree import ElementTree as ET

# --------------------------------------------------------------------------
# Normalización de nombres de herramienta
# --------------------------------------------------------------------------

# Los Plugin de Alteryx vienen como "<Assembly>.<Tool>.<Tool>".
# Ej: AlteryxBasePluginsGui.Filter.Filter -> Filter
# Algunos nombres internos no coinciden con el nombre visible en el canvas,
# así que los mapeamos explícitamente. Esta tabla se amplía sobre la marcha.
PLUGIN_ALIASES = {
    "DbFileInput": "Input Data",
    "DbFileOutput": "Output Data",
    "AlteryxSelect": "Select",
    "AppendFields": "Append Fields",
    "MultiRowFormula": "Multi-Row Formula",
    "MultiFieldFormula": "Multi-Field Formula",
    "MultiFieldBinary": "Multi-Field Binary",
    "CrossTab": "Cross Tab",
    "TextToColumns": "Text To Columns",
    "RecordID": "Record ID",
    "DynamicRename": "Dynamic Rename",
    "DynamicSelect": "Dynamic Select",
    "DynamicInput": "Dynamic Input",
    "FindReplace": "Find Replace",
    "RunningTotal": "Running Total",
    "DateTimeNow": "DateTime",
    "TextInput": "Text Input",
    "JoinMultiple": "Join Multiple",
    "MakeColumns": "Make Columns",
    "FieldInfo": "Field Info",
    "GenerateRows": "Generate Rows",
    "RandomRecords": "Random % Sample",
    "BlockUntilDone": "Block Until Done",
    "ToolContainer": "__container__",
}

# Herramientas que no producen datos y no requieren traducción a PySpark.
COSMETIC_TOOLS = {"__container__", "Comment", "TextBox", "ExplorerBox", "Browse", "BrowseV2"}


def basename(p: str) -> str:
    """Path().name no parte backslashes en Linux; las rutas de Alteryx son Windows."""
    return p.replace("\\", "/").rstrip("/").split("/")[-1]


def normalize_plugin(plugin: str) -> str:
    """AlteryxBasePluginsGui.Filter.Filter -> 'Filter'."""
    if not plugin:
        return "__unknown__"
    parts = plugin.split(".")
    raw = parts[-2] if len(parts) >= 2 else parts[-1]
    return PLUGIN_ALIASES.get(raw, raw)


# --------------------------------------------------------------------------
# Modelo
# --------------------------------------------------------------------------


@dataclass
class WorkflowStats:
    path: str
    name: str
    parsed_ok: bool = True
    parse_error: str = ""
    tool_counts: dict[str, int] = field(default_factory=dict)
    macro_refs: list[str] = field(default_factory=list)
    connection_count: int = 0
    named_outputs: dict[str, int] = field(default_factory=dict)  # J / L / R etc.
    expression_count: int = 0
    expressions_sample: list[str] = field(default_factory=list)
    container_count: int = 0
    max_depth: int = 0

    @property
    def data_tool_count(self) -> int:
        return sum(n for t, n in self.tool_counts.items() if t not in COSMETIC_TOOLS)

    @property
    def distinct_tools(self) -> set[str]:
        return {t for t in self.tool_counts if t not in COSMETIC_TOOLS}

    @property
    def complexity(self) -> float:
        """Heurística simple para ordenar workflows y elegir piloto."""
        return (
            self.data_tool_count
            + 0.5 * self.connection_count
            + 2.0 * self.expression_count
            + 5.0 * len(self.macro_refs)
        )


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


def iter_workflow_sources(root: Path):
    """Produce (etiqueta, bytes_xml) para cada workflow bajo `root`."""
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        suffix = p.suffix.lower()
        if suffix in (".yxmd", ".yxmc"):
            yield str(p), p.read_bytes()
        elif suffix == ".yxzp":
            # Un .yxzp es un zip; el workflow principal es el .yxmd que trae dentro.
            try:
                with zipfile.ZipFile(p) as zf:
                    for member in zf.namelist():
                        if member.lower().endswith((".yxmd", ".yxmc")):
                            yield f"{p}!{member}", zf.read(member)
            except zipfile.BadZipFile as exc:
                yield str(p), b""  # se reportará como error de parseo
                print(f"  [warn] no se pudo abrir {p}: {exc}", file=sys.stderr)


def collect_expressions(node: ET.Element) -> list[str]:
    """
    Recoge expresiones del lenguaje Formula de Alteryx dondequiera que aparezcan.
    Formula/Multi-Row/Multi-Field las guardan en @expression; Filter en <Expression>.
    """
    found = []
    for el in node.iter():
        expr = el.get("expression")
        if expr and expr.strip():
            found.append(expr.strip())
        if el.tag.endswith("Expression") and el.text and el.text.strip():
            found.append(el.text.strip())
    return found


def walk_nodes(nodes_el: ET.Element, stats: WorkflowStats, depth: int = 0) -> None:
    """Recorre <Nodes>, entrando en los ChildNodes de los contenedores."""
    stats.max_depth = max(stats.max_depth, depth)
    counts = Counter(stats.tool_counts)

    for node in nodes_el.findall("Node"):
        gui = node.find("GuiSettings")
        plugin = gui.get("Plugin", "") if gui is not None else ""
        engine = node.find("EngineSettings")
        macro = engine.get("Macro", "") if engine is not None else ""

        if macro:
            tool_name = f"MACRO::{basename(macro)}"
            stats.macro_refs.append(macro)
        else:
            tool_name = normalize_plugin(plugin)

        counts[tool_name] += 1
        if tool_name == "__container__":
            stats.container_count += 1

        props = node.find("Properties")
        if props is not None:
            exprs = collect_expressions(props)
            stats.expression_count += len(exprs)
            for e in exprs:
                if len(stats.expressions_sample) < 40:
                    stats.expressions_sample.append(e)

        child = node.find("ChildNodes")
        if child is not None:
            stats.tool_counts = dict(counts)
            walk_nodes(child, stats, depth + 1)
            counts = Counter(stats.tool_counts)

    stats.tool_counts = dict(counts)


def parse_workflow(label: str, raw: bytes) -> WorkflowStats:
    stats = WorkflowStats(path=label, name=Path(label.split("!")[0]).name)
    if not raw:
        stats.parsed_ok = False
        stats.parse_error = "archivo vacío o ilegible"
        return stats
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        stats.parsed_ok = False
        stats.parse_error = str(exc)
        return stats

    nodes_el = root.find("Nodes")
    if nodes_el is not None:
        walk_nodes(nodes_el, stats)

    conns_el = root.find("Connections")
    if conns_el is not None:
        named = Counter()
        for conn in conns_el.findall("Connection"):
            stats.connection_count += 1
            origin = conn.find("Origin")
            if origin is not None:
                cname = origin.get("Connection", "")
                # J/L/R en Join, T/F en Filter, etc. Señalan lógica que hay que inferir.
                if cname and cname not in ("Output", ""):
                    named[cname] += 1
        stats.named_outputs = dict(named)

    return stats


# --------------------------------------------------------------------------
# Reporte
# --------------------------------------------------------------------------


def build_report(all_stats: list[WorkflowStats], top_n: int) -> dict:
    ok = [s for s in all_stats if s.parsed_ok]
    failed = [s for s in all_stats if not s.parsed_ok]

    tool_total = Counter()
    tool_workflows = Counter()  # en cuántos workflows distintos aparece
    for s in ok:
        for tool, n in s.tool_counts.items():
            if tool in COSMETIC_TOOLS:
                continue
            tool_total[tool] += n
            tool_workflows[tool] += 1

    ranked = tool_total.most_common()
    total_instances = sum(tool_total.values()) or 1

    # Cobertura acumulada: si soporto las primeras K herramientas,
    # ¿qué % de instancias cubro y cuántos workflows quedan 100% cubiertos?
    coverage = []
    running = 0
    for i, (tool, n) in enumerate(ranked, start=1):
        running += n
        supported = {t for t, _ in ranked[:i]}
        fully = sum(1 for s in ok if s.distinct_tools <= supported)
        coverage.append(
            {
                "top_k": i,
                "tool_added": tool,
                "pct_instances": round(100 * running / total_instances, 1),
                "workflows_fully_covered": fully,
                "pct_workflows_fully_covered": round(100 * fully / max(len(ok), 1), 1),
            }
        )

    macro_counter = Counter()
    for s in ok:
        for m in s.macro_refs:
            macro_counter[basename(m)] += 1

    # Salidas nombradas: J/L/R en Join, True/False en Filter. Son la señal de
    # que hay lógica de topología que inferir, no solo una traducción 1:1.
    named_out = Counter()
    for s in ok:
        for k, n in s.named_outputs.items():
            named_out[k] += n

    complexities = sorted(ok, key=lambda s: s.complexity)
    pilot = None
    if complexities:
        # Piloto = mediana de complejidad, sin macros custom.
        candidates = [s for s in complexities if not s.macro_refs] or complexities
        pilot = candidates[len(candidates) // 2]

    return {
        "summary": {
            "workflows_found": len(all_stats),
            "workflows_parsed": len(ok),
            "workflows_failed": len(failed),
            "distinct_tools": len(tool_total),
            "total_tool_instances": total_instances,
            "total_expressions": sum(s.expression_count for s in ok),
            "workflows_with_macros": sum(1 for s in ok if s.macro_refs),
            "distinct_macros": len(macro_counter),
            "median_tools_per_workflow": (
                statistics.median([s.data_tool_count for s in ok]) if ok else 0
            ),
        },
        "top_tools": [
            {
                "tool": t,
                "instances": n,
                "workflows": tool_workflows[t],
                "pct_of_instances": round(100 * n / total_instances, 1),
            }
            for t, n in ranked[:top_n]
        ],
        "all_tools": [{"tool": t, "instances": n} for t, n in ranked],
        "coverage_curve": coverage,
        "macros": [{"macro": m, "uses": n} for m, n in macro_counter.most_common()],
        "named_outputs": [{"output": k, "count": n} for k, n in named_out.most_common()],
        "pilot_candidate": (
            {"path": pilot.path, "complexity": round(pilot.complexity, 1),
             "tools": pilot.data_tool_count, "expressions": pilot.expression_count}
            if pilot else None
        ),
        "hardest_workflows": [
            {"path": s.path, "complexity": round(s.complexity, 1),
             "tools": s.data_tool_count, "expressions": s.expression_count,
             "macros": len(s.macro_refs)}
            for s in complexities[::-1][:10]
        ],
        "parse_failures": [{"path": s.path, "error": s.parse_error} for s in failed],
    }


def write_markdown(report: dict, out: Path) -> None:
    s = report["summary"]
    lines = [
        "# Inventario de workflows Alteryx",
        "",
        f"- Workflows encontrados: **{s['workflows_found']}** "
        f"(parseados {s['workflows_parsed']}, fallidos {s['workflows_failed']})",
        f"- Herramientas distintas en uso: **{s['distinct_tools']}**",
        f"- Instancias totales de herramienta: **{s['total_tool_instances']}**",
        f"- Mediana de herramientas por workflow: **{s['median_tools_per_workflow']}**",
        f"- Expresiones Formula a transpilar: **{s['total_expressions']}**",
        f"- Workflows con macros custom: **{s['workflows_with_macros']}** "
        f"({s['distinct_macros']} macros distintas)",
        "",
        "## Herramientas más usadas",
        "",
        "| # | Herramienta | Instancias | Workflows | % del total |",
        "|---|---|---|---|---|",
    ]
    for i, t in enumerate(report["top_tools"], 1):
        lines.append(
            f"| {i} | `{t['tool']}` | {t['instances']} | {t['workflows']} | {t['pct_of_instances']}% |"
        )

    lines += ["", "## Curva de cobertura", "",
              "Cuántos workflows quedan 100% cubiertos según cuántas herramientas soportemos.",
              "", "| Top-K | Última añadida | % instancias | Workflows completos |", "|---|---|---|---|"]
    for row in report["coverage_curve"]:
        if row["top_k"] % 5 == 0 or row["top_k"] <= 10:
            lines.append(
                f"| {row['top_k']} | `{row['tool_added']}` | {row['pct_instances']}% | "
                f"{row['workflows_fully_covered']} ({row['pct_workflows_fully_covered']}%) |"
            )

    if report["named_outputs"]:
        lines += ["", "## Salidas nombradas (lógica a inferir)", "",
                  "`Join`/`Left`/`Right` en un Join y `True`/`False` en un Filter "
                  "indican ramas que hay que reconstruir en Spark, no traducir 1:1.",
                  "", "| Salida | Conexiones |", "|---|---|"]
        for o in report["named_outputs"]:
            lines.append(f"| `{o['output']}` | {o['count']} |")

    if report["macros"]:
        lines += ["", "## Macros custom (revisión manual obligatoria)", "",
                  "| Macro | Usos |", "|---|---|"]
        for m in report["macros"][:30]:
            lines.append(f"| `{m['macro']}` | {m['uses']} |")

    if report["pilot_candidate"]:
        p = report["pilot_candidate"]
        lines += ["", "## Piloto sugerido", "",
                  f"`{p['path']}` — complejidad {p['complexity']}, "
                  f"{p['tools']} herramientas, {p['expressions']} expresiones.",
                  "", "Complejidad mediana y sin macros custom: buen primer caso "
                  "para migrar a mano y usar como referencia de paridad."]

    if report["parse_failures"]:
        lines += ["", "## Archivos que no se pudieron parsear", ""]
        for f in report["parse_failures"]:
            lines.append(f"- `{f['path']}`: {f['error']}")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Inventario de workflows Alteryx")
    ap.add_argument("root", type=Path, help="Directorio con .yxmd / .yxzp")
    ap.add_argument("--out", type=Path, default=Path("./inventory"))
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    if not args.root.exists():
        print(f"No existe: {args.root}", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)

    all_stats = []
    for label, raw in iter_workflow_sources(args.root):
        all_stats.append(parse_workflow(label, raw))

    if not all_stats:
        print("No se encontraron archivos .yxmd / .yxzp / .yxmc", file=sys.stderr)
        return 1

    report = build_report(all_stats, args.top)

    (args.out / "inventory.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    write_markdown(report, args.out / "inventory.md")

    with (args.out / "tools.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["tool", "instances"])
        for t in report["all_tools"]:
            w.writerow([t["tool"], t["instances"]])

    with (args.out / "workflows.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["path", "tools", "connections", "expressions", "macros", "complexity"])
        for s in sorted(all_stats, key=lambda x: -x.complexity):
            w.writerow([s.path, s.data_tool_count, s.connection_count,
                        s.expression_count, len(s.macro_refs), round(s.complexity, 1)])

    # Todas las expresiones, para dimensionar y luego construir el transpilador.
    with (args.out / "expressions.txt").open("w", encoding="utf-8") as fh:
        for s in all_stats:
            for e in s.expressions_sample:
                fh.write(f"{s.name}\t{e}\n")

    sm = report["summary"]
    print(f"OK  {sm['workflows_parsed']}/{sm['workflows_found']} workflows parseados")
    print(f"    {sm['distinct_tools']} herramientas distintas, "
          f"{sm['total_expressions']} expresiones, "
          f"{sm['distinct_macros']} macros custom")
    print(f"    Reporte en {args.out / 'inventory.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
