#!/usr/bin/env python3
"""
Agrega los reportes de caso (cases/*/reporte.json) para detectar patrones
repetidos entre migraciones reales -- que herramienta sin soporte aparece
mas seguido, que codigo de warning se repite -- y asi priorizar que mejorar
primero en la skill en vez de adivinar.

Uso:
    python scripts/summarize_cases.py
    python scripts/summarize_cases.py --cases-dir /ruta/a/cases
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def load_cases(cases_dir: Path) -> list[dict]:
    cases = []
    for p in sorted(cases_dir.glob("*/reporte.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[warn] no se pudo leer {p}: {exc}")
            continue
        data["_path"] = str(p)
        cases.append(data)
    return cases


def summarize(cases: list[dict]) -> str:
    if not cases:
        return "No hay casos en cases/ todavia -- se van acumulando conforme el equipo migra workflows reales."

    tool_counter: Counter = Counter()
    warning_counter: Counter = Counter()
    total_nodos = total_con_generador = total_todos = 0

    for c in cases:
        tool_counter.update(c.get("herramientas_sin_soporte", []) or [])
        warning_counter.update(c.get("warnings_por_codigo", {}) or {})
        total_nodos += c.get("nodos_totales") or 0
        total_con_generador += c.get("nodos_con_generador") or 0
        total_todos += c.get("todos_pendientes") or 0

    lines = [
        f"# Resumen de {len(cases)} caso(s) en cases/",
        "",
        f"- Nodos migrados en total (acumulado): {total_nodos}",
        f"- Nodos con generador automatico (acumulado): {total_con_generador}",
        f"- TODOs pendientes (acumulado): {total_todos}",
        "",
        "## Herramientas sin soporte mas frecuentes",
        "",
        "Candidatas a priorizar en `references/tool_mapping.md` / `generate_notebook.py`.",
        "",
        "| Herramienta | En cuantos casos aparece |",
        "|---|---|",
    ]
    for tool, n in tool_counter.most_common():
        lines.append(f"| `{tool}` | {n} |")
    if not tool_counter:
        lines.append("| (ninguna) | - |")

    lines += [
        "",
        "## Codigos de warning mas frecuentes",
        "",
        "| Codigo | Ocurrencias totales |",
        "|---|---|",
    ]
    for code, n in warning_counter.most_common():
        lines.append(f"| `{code}` | {n} |")
    if not warning_counter:
        lines.append("| (ninguno) | - |")

    lines += [
        "",
        "## Casos individuales",
        "",
        "| Fecha | Workflow | Modo | TODOs | Resultado |",
        "|---|---|---|---|---|",
    ]
    for c in sorted(cases, key=lambda c: c.get("fecha", "")):
        lines.append(
            f"| {c.get('fecha', '?')} | {c.get('workflow', '?')} | {c.get('modo', '?')} | "
            f"{c.get('todos_pendientes', '?')} | {c.get('resultado', '?')} |"
        )

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Resume los reportes de caso en cases/ para detectar patrones de mejora")
    ap.add_argument(
        "--cases-dir",
        type=Path,
        default=Path(__file__).resolve().parents[4] / "cases",
        help="Carpeta cases/ a resumir (default: la raiz del repo)",
    )
    args = ap.parse_args()

    if not args.cases_dir.exists():
        print(f"No existe {args.cases_dir} -- todavia no hay casos guardados.")
        return 0

    print(summarize(load_cases(args.cases_dir)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
