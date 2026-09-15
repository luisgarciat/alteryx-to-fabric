#!/usr/bin/env python3
"""
Validación de paridad Alteryx <-> PySpark (Fase 5).

Compara un dataset de referencia (la salida real de Alteryx para un conjunto
de prueba) contra el dataset que produjo el notebook generado en la Fase 4,
y calcula un índice ponderado de equivalencia. Ver references/parity.md para
la definición completa de cada métrica y el significado del índice.

Diseño: separa "calcular estadísticas de un dataset" (`compute_stats_*`) de
"comparar dos conjuntos de estadísticas" (`compare`). Lo segundo es lógica
pura -- sin Spark, sin IO -- y es lo que de verdad hay que confiar; por eso
es lo que este módulo prueba exhaustivamente. Hay dos formas de llegar a un
"stats dict":

  - `compute_stats_from_records(records, config)` — puro Python, sirve para
    pilotos chicos (CSV de referencia vs CSV de salida) sin necesitar un
    cluster Spark. Es lo que usan las pruebas de este archivo.
  - `compute_stats_spark(df, config)` — requiere una sesión Spark activa
    (Fabric/Databricks); agrega en el motor, nunca hace `.collect()` de las
    filas completas.

Uso típico en un test de un workflow migrado (ver test_parity_example.py):

    from parity import assert_parity, load_csv_as_records

    def test_ventas_parity():
        ref = load_csv_as_records("referencia/ventas_alteryx.csv")
        cand = load_csv_as_records("salida/ventas_spark.csv")
        assert_parity(ref, cand, config={
            "metrics": {"aggregates": {"rules": [
                {"func": "sum", "column": "monto"},
                {"func": "count", "column": "cliente_id"},
            ]}}
        })
"""

from __future__ import annotations

import copy
import csv
from pathlib import Path

# --------------------------------------------------------------------------
# Config por default — mismos nombres y pesos que 10_paridad_alteryx_pyspark.md
# (paquete GPT original). Los pesos se renormalizan entre las métricas
# habilitadas, así que no hace falta que sumen 1.0 si deshabilitas alguna.
# --------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "tolerances": {"numeric_abs": 0.0001, "numeric_rel": 0.001},
    "weights": {
        "row_count": 0.35,
        "aggregates": 0.35,
        "schema_types": 0.15,
        "null_integrity": 0.10,
        "value_domains": 0.05,
    },
    "metrics": {
        "row_count": {"enabled": True},
        "aggregates": {"enabled": True, "rules": []},  # [{"func": "sum", "column": "monto"}, ...]
        "schema_types": {"enabled": True},
        "null_integrity": {"enabled": True, "required_cols": []},
        "value_domains": {"enabled": False, "columns": {}},  # {"status": ["A", "B", "C"]}
    },
}


def merge_config(overrides: dict | None) -> dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    if not overrides:
        return cfg
    for section in ("tolerances", "weights"):
        cfg[section].update(overrides.get(section, {}))
    for metric, values in overrides.get("metrics", {}).items():
        cfg["metrics"].setdefault(metric, {}).update(values)
    return cfg


# --------------------------------------------------------------------------
# compute_stats_from_records — puro Python, sin Spark
# --------------------------------------------------------------------------


def _agg_key(rule: dict) -> str:
    return f"{rule['func']}__{rule['column']}"


def _aggregate(records: list[dict], func: str, column: str):
    values = [r[column] for r in records if r.get(column) is not None]
    if func == "sum":
        return sum(values) if values else 0
    if func in ("avg", "mean"):
        return (sum(values) / len(values)) if values else None
    if func == "min":
        return min(values) if values else None
    if func == "max":
        return max(values) if values else None
    if func == "count":
        return len(values)
    if func in ("countdistinct", "count_distinct"):
        return len(set(values))
    raise ValueError(f"Funcion de agregacion no soportada: {func}")


def _infer_type(records: list[dict], column: str) -> str:
    for r in records:
        v = r.get(column)
        if v is not None:
            return type(v).__name__
    return "NoneType"


def compute_stats_from_records(records: list[dict], config: dict | None = None) -> dict:
    cfg = merge_config(config)
    columns = sorted({k for r in records for k in r.keys()})
    total = len(records)

    aggregates = {}
    if cfg["metrics"]["aggregates"]["enabled"]:
        for rule in cfg["metrics"]["aggregates"]["rules"]:
            aggregates[_agg_key(rule)] = _aggregate(records, rule["func"], rule["column"])

    dtypes = {}
    if cfg["metrics"]["schema_types"]["enabled"]:
        dtypes = {c: _infer_type(records, c) for c in columns}

    null_counts = {}
    if cfg["metrics"]["null_integrity"]["enabled"]:
        for c in cfg["metrics"]["null_integrity"]["required_cols"]:
            null_counts[c] = sum(1 for r in records if r.get(c) is None)

    domain_violations = {}
    if cfg["metrics"]["value_domains"]["enabled"]:
        for col, allowed in cfg["metrics"]["value_domains"]["columns"].items():
            allowed_set = set(allowed)
            non_null = [r[col] for r in records if r.get(col) is not None]
            domain_violations[col] = {
                "violations": sum(1 for v in non_null if v not in allowed_set),
                "total": len(non_null),
            }

    return {
        "row_count": total,
        "total": total,
        "dtypes": dtypes,
        "aggregates": aggregates,
        "null_counts": null_counts,
        "domain_violations": domain_violations,
    }


def compute_stats_spark(df, config: dict | None = None) -> dict:
    """Requiere una sesion Spark activa (Fabric/Databricks) — no se puede
    ejercitar en pruebas locales sin cluster. Agrega en el motor: nunca
    hace collect() de las filas completas, solo de los resultados agregados."""
    from pyspark.sql import functions as F  # import perezoso: solo hace falta en Fabric

    cfg = merge_config(config)
    total = df.count()
    dtypes = dict(df.dtypes) if cfg["metrics"]["schema_types"]["enabled"] else {}

    aggregates = {}
    agg_cfg = cfg["metrics"]["aggregates"]
    if agg_cfg["enabled"] and agg_cfg["rules"]:
        exprs = []
        for rule in agg_cfg["rules"]:
            fn, col, key = rule["func"], rule["column"], _agg_key(rule)
            if fn == "sum":
                exprs.append(F.sum(col).alias(key))
            elif fn in ("avg", "mean"):
                exprs.append(F.avg(col).alias(key))
            elif fn == "min":
                exprs.append(F.min(col).alias(key))
            elif fn == "max":
                exprs.append(F.max(col).alias(key))
            elif fn == "count":
                exprs.append(F.count(col).alias(key))
            elif fn in ("countdistinct", "count_distinct"):
                exprs.append(F.countDistinct(col).alias(key))
            else:
                raise ValueError(f"Funcion de agregacion no soportada: {fn}")
        row = df.agg(*exprs).collect()[0].asDict()
        aggregates = row

    null_counts = {}
    ni_cfg = cfg["metrics"]["null_integrity"]
    if ni_cfg["enabled"]:
        for c in ni_cfg["required_cols"]:
            null_counts[c] = df.filter(F.col(c).isNull()).count()

    domain_violations = {}
    vd_cfg = cfg["metrics"]["value_domains"]
    if vd_cfg["enabled"]:
        for col, allowed in vd_cfg["columns"].items():
            non_null_total = df.filter(F.col(col).isNotNull()).count()
            violations = df.filter(F.col(col).isNotNull() & (~F.col(col).isin(list(allowed)))).count()
            domain_violations[col] = {"violations": violations, "total": non_null_total}

    return {
        "row_count": total, "total": total, "dtypes": dtypes,
        "aggregates": aggregates, "null_counts": null_counts, "domain_violations": domain_violations,
    }


# --------------------------------------------------------------------------
# Métricas individuales — cada una devuelve {"score": 0..1, "detail": {...}}
# --------------------------------------------------------------------------


def metric_row_count(stats_a: dict, stats_b: dict) -> dict:
    a, b = stats_a["row_count"], stats_b["row_count"]
    diff_pct = abs(a - b) / max(a, 1)
    return {"score": max(0.0, 1.0 - min(1.0, diff_pct)), "detail": {"reference": a, "candidate": b, "diff_pct": round(diff_pct, 6)}}


def _within_tolerance(a, b, tol: dict) -> bool:
    if a is None or b is None:
        return a == b
    diff = abs(a - b)
    return diff <= tol["numeric_abs"] or diff <= tol["numeric_rel"] * max(abs(a), abs(b), 1)


def metric_aggregates(stats_a: dict, stats_b: dict, tolerances: dict) -> dict:
    keys = sorted(set(stats_a["aggregates"]) | set(stats_b["aggregates"]))
    if not keys:
        return {"score": 1.0, "detail": {"rules": [], "note": "sin reglas de agregacion configuradas"}}
    rows, passed = [], 0
    for k in keys:
        va, vb = stats_a["aggregates"].get(k), stats_b["aggregates"].get(k)
        ok = _within_tolerance(va, vb, tolerances)
        passed += int(ok)
        rows.append({"rule": k, "reference": va, "candidate": vb, "ok": ok})
    return {"score": passed / len(keys), "detail": {"rules": rows}}


def metric_schema_types(stats_a: dict, stats_b: dict) -> dict:
    cols_a, cols_b = stats_a["dtypes"], stats_b["dtypes"]
    common = sorted(set(cols_a) & set(cols_b))
    missing = sorted(set(cols_a) ^ set(cols_b))
    if not common and not missing:
        return {"score": 1.0, "detail": {"note": "schema_types deshabilitado o sin columnas"}}
    mismatches = [c for c in common if cols_a[c] != cols_b[c]]
    total = len(common) + len(missing)
    ok = len(common) - len(mismatches)
    return {
        "score": ok / total if total else 1.0,
        "detail": {"mismatches": {c: (cols_a[c], cols_b[c]) for c in mismatches}, "missing_on_either_side": missing},
    }


def metric_null_integrity(stats_a: dict, stats_b: dict, total_a: int, total_b: int, tolerances: dict) -> dict:
    cols = sorted(set(stats_a["null_counts"]) | set(stats_b["null_counts"]))
    if not cols:
        return {"score": 1.0, "detail": {"note": "sin required_cols configuradas"}}
    passed, rows = 0, []
    for c in cols:
        rate_a = stats_a["null_counts"].get(c, 0) / max(total_a, 1)
        rate_b = stats_b["null_counts"].get(c, 0) / max(total_b, 1)
        ok = _within_tolerance(rate_a, rate_b, tolerances)
        passed += int(ok)
        rows.append({"column": c, "null_rate_reference": round(rate_a, 6), "null_rate_candidate": round(rate_b, 6), "ok": ok})
    return {"score": passed / len(cols), "detail": {"columns": rows}}


def metric_value_domains(stats_a: dict, stats_b: dict) -> dict:
    cols = sorted(set(stats_a["domain_violations"]) | set(stats_b["domain_violations"]))
    if not cols:
        return {"score": 1.0, "detail": {"note": "value_domains deshabilitado o sin columnas"}}
    rows, diffs = [], []
    for c in cols:
        va = stats_a["domain_violations"].get(c, {"violations": 0, "total": 1})
        vb = stats_b["domain_violations"].get(c, {"violations": 0, "total": 1})
        rate_a = va["violations"] / max(va["total"], 1)
        rate_b = vb["violations"] / max(vb["total"], 1)
        diffs.append(abs(rate_a - rate_b))
        rows.append({"column": c, "violation_rate_reference": round(rate_a, 6), "violation_rate_candidate": round(rate_b, 6)})
    score = max(0.0, 1.0 - (sum(diffs) / len(diffs)))
    return {"score": score, "detail": {"columns": rows}}


# --------------------------------------------------------------------------
# Índice ponderado y veredicto
# --------------------------------------------------------------------------


def classify(index: float) -> str:
    if index >= 0.99:
        return "SUCCESS"
    if index >= 0.95:
        return "REVIEW"
    return "FAIL"


def compare(stats_a: dict, stats_b: dict, config: dict | None = None) -> dict:
    """stats_a = referencia (Alteryx), stats_b = candidato (PySpark generado).
    Ambos deben venir del MISMO metodo de computo (los dos de
    compute_stats_from_records, o los dos de compute_stats_spark) — mezclar
    fuentes hace que schema_types compare vocabularios de tipos distintos."""
    cfg = merge_config(config)
    tol = cfg["tolerances"]
    metrics_cfg = cfg["metrics"]

    results = {}
    if metrics_cfg["row_count"]["enabled"]:
        results["row_count"] = metric_row_count(stats_a, stats_b)
    if metrics_cfg["aggregates"]["enabled"]:
        results["aggregates"] = metric_aggregates(stats_a, stats_b, tol)
    if metrics_cfg["schema_types"]["enabled"]:
        results["schema_types"] = metric_schema_types(stats_a, stats_b)
    if metrics_cfg["null_integrity"]["enabled"]:
        results["null_integrity"] = metric_null_integrity(stats_a, stats_b, stats_a["total"], stats_b["total"], tol)
    if metrics_cfg["value_domains"]["enabled"]:
        results["value_domains"] = metric_value_domains(stats_a, stats_b)

    weights = cfg["weights"]
    total_weight = sum(weights[m] for m in results) or 1.0
    index = sum(results[m]["score"] * weights[m] for m in results) / total_weight

    return {"index": round(index, 6), "verdict": classify(index), "metrics": results}


def format_report(result: dict) -> str:
    lines = [f"Indice de paridad: {result['index']:.4f} -> {result['verdict']}"]
    for name, m in result["metrics"].items():
        lines.append(f"  - {name}: score={m['score']:.4f}  {m['detail']}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# API de conveniencia para pytest
# --------------------------------------------------------------------------


def assert_parity(reference_records: list[dict], candidate_records: list[dict], config: dict | None = None) -> dict:
    stats_a = compute_stats_from_records(reference_records, config)
    stats_b = compute_stats_from_records(candidate_records, config)
    result = compare(stats_a, stats_b, config)
    print(format_report(result))
    if result["verdict"] == "FAIL":
        raise AssertionError(f"Paridad FAIL (indice={result['index']:.4f}):\n{format_report(result)}")
    return result


def assert_parity_spark(df_reference, df_candidate, config: dict | None = None) -> dict:
    stats_a = compute_stats_spark(df_reference, config)
    stats_b = compute_stats_spark(df_candidate, config)
    result = compare(stats_a, stats_b, config)
    print(format_report(result))
    if result["verdict"] == "FAIL":
        raise AssertionError(f"Paridad FAIL (indice={result['index']:.4f}):\n{format_report(result)}")
    return result


def load_csv_as_records(path: str | Path) -> list[dict]:
    """CSV -> list[dict] con coerción ligera de tipos (int, luego float, si no
    texto; celda vacia -> None). Para pilotos chicos sin Spark; en Fabric usa
    compute_stats_spark sobre un DataFrame real en su lugar."""
    records = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            parsed = {}
            for k, v in row.items():
                if v == "" or v is None:
                    parsed[k] = None
                    continue
                try:
                    parsed[k] = int(v)
                    continue
                except ValueError:
                    pass
                try:
                    parsed[k] = float(v)
                    continue
                except ValueError:
                    pass
                parsed[k] = v
            records.append(parsed)
    return records
