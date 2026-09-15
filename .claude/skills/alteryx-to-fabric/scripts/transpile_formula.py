#!/usr/bin/env python3
"""
Aplica formula_transpiler.py sobre el modelo intermedio JSON que produce
parse_workflow.py (Fase 2): busca nodos Formula / Multi-Field Formula /
Filter / Multi-Row Formula, transpila sus expresiones a PySpark, y escribe
un IR aumentado con `pyspark_code` listo para que la Fase 4 lo inyecte en
el notebook.

Uso:
    # una expresion suelta, para probar rapido sin armar un IR completo
    python scripts/transpile_formula.py --expr 'IIF([monto]>100,"alto","bajo")'

    # un IR completo (salida de parse_workflow.py)
    python scripts/transpile_formula.py flujo.ir.json
    python scripts/transpile_formula.py flujo.ir.json --out flujo.transpiled.json

Para Multi-Row Formula, la variable de ventana se nombra por convención
`w_<ToolID>` — la Fase 4 es responsable de definir esa `Window` (partition
por el `group_by` del nodo, order por la columna que se confirme con el
usuario; ver WARN_ORDER_UNDEFINED en warnings.md). Este script no la define,
solo genera código que asume que existirá con ese nombre.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from formula_transpiler import transpile

EXPRESSION_LIST_TOOLS = {"Formula", "Multi-Field Formula"}
SINGLE_EXPRESSION_TOOLS = {"Filter"}
ROW_AWARE_TOOLS = {"Multi-Row Formula"}


def record_warnings(ir: dict, tool_id: str, tool_type: str, warnings: list[dict]) -> None:
    for w in warnings:
        ir["metadata"]["warnings"].append(
            {"code": w["code"], "tool_id": tool_id, "tool": tool_type, "message": w["message"]}
        )


def process_node(ir: dict, node: dict) -> int:
    """Transpila las expresiones de un nodo in-place. Devuelve cuántas transpiló."""
    tool_type = node["type"]
    params = node["params"]
    count = 0

    if tool_type in EXPRESSION_LIST_TOOLS:
        for entry in params.get("expressions", []):
            result = transpile(entry["expr"])
            entry["pyspark_code"] = result.code
            if result.warnings:
                entry["warnings"] = result.warnings
                record_warnings(ir, node["id"], tool_type, result.warnings)
            count += 1

    elif tool_type in SINGLE_EXPRESSION_TOOLS:
        expr = params.get("expression", "")
        if expr:
            result = transpile(expr)
            params["pyspark_code"] = result.code
            if result.warnings:
                params["warnings"] = result.warnings
                record_warnings(ir, node["id"], tool_type, result.warnings)
            count += 1

    elif tool_type in ROW_AWARE_TOOLS:
        expr = params.get("expression", "")
        if expr:
            window_var = f"w_{node['id']}"
            result = transpile(expr, window_var=window_var)
            params["pyspark_code"] = result.code
            params["window_var"] = window_var
            if result.warnings:
                params["warnings"] = result.warnings
                record_warnings(ir, node["id"], tool_type, result.warnings)
            count += 1

    return count


def main() -> int:
    ap = argparse.ArgumentParser(description="Transpila expresiones Formula de Alteryx a PySpark")
    ap.add_argument("path", type=Path, nargs="?", help="IR JSON (salida de parse_workflow.py)")
    ap.add_argument("--expr", help="Transpila una expresión suelta y sale, sin necesitar un IR")
    ap.add_argument("--window-var", default=None, help="Variable Window a usar con --expr (para [Row-n:Campo])")
    ap.add_argument("--out", type=Path, default=None, help="Archivo de salida (default: <nombre>.transpiled.json)")
    args = ap.parse_args()

    if args.expr is not None:
        result = transpile(args.expr, window_var=args.window_var)
        print(result.code)
        for w in result.warnings:
            print(f"[{w['code']}] {w['message']}", file=sys.stderr)
        return 0

    if args.path is None:
        ap.error("pasa un archivo IR JSON, o usa --expr 'expresion' para probar una expresión suelta")

    if not args.path.exists():
        print(f"No existe: {args.path}", file=sys.stderr)
        return 1

    ir = json.loads(args.path.read_text(encoding="utf-8"))
    ir.setdefault("metadata", {}).setdefault("warnings", [])
    warnings_before = len(ir["metadata"]["warnings"])

    total = 0
    for node in ir.get("nodes", []):
        total += process_node(ir, node)

    out_path = args.out or args.path.with_name(args.path.stem.replace(".ir", "") + ".transpiled.json")
    out_path.write_text(json.dumps(ir, indent=2, ensure_ascii=False), encoding="utf-8")

    for w in ir["metadata"]["warnings"][warnings_before:]:
        print(f"  [{w['code']}] ToolID={w['tool_id']} ({w['tool']}): {w['message']}")

    print(f"OK  {total} expresiones transpiladas -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
