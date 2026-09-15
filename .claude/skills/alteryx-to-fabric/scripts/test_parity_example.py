"""
Ejemplo runnable de cómo validar la paridad de un workflow migrado, y prueba
de regresión de parity.py contra los fixtures en fixtures/parity/.

Correr con:
    pytest scripts/test_parity_example.py -v

Para un workflow real, copia el patrón de test_ventas_parity_ejemplo() a un
archivo propio (ej. test_ventas_parity.py) apuntando a:
  - un CSV/Parquet exportado de la corrida real de Alteryx (la referencia)
  - la salida del notebook generado en la Fase 4 para el mismo input
y ajusta `config` con las reglas de agregación y columnas relevantes de ESE
workflow (ver references/parity.md).
"""

from pathlib import Path

import pytest
from parity import (
    assert_parity,
    compare,
    compute_stats_from_records,
    load_csv_as_records,
    metric_row_count,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "parity"

CONFIG = {
    "metrics": {
        "aggregates": {
            "rules": [
                {"func": "sum", "column": "monto"},
                {"func": "avg", "column": "monto"},
                {"func": "count", "column": "cliente_id"},
            ]
        },
        "null_integrity": {"required_cols": ["id", "cliente_id", "segmento", "status"]},
    }
}


def test_ventas_parity_ejemplo():
    """Patrón a copiar por workflow real: cargar referencia y candidato, llamar
    assert_parity. Si el veredicto es FAIL, el assert lanza con el detalle
    completo — no hace falta revisar nada a mano para detectarlo."""
    reference = load_csv_as_records(FIXTURES / "reference.csv")
    candidate = load_csv_as_records(FIXTURES / "candidate_success.csv")
    result = assert_parity(reference, candidate, config=CONFIG)
    assert result["verdict"] == "SUCCESS"


def test_review_no_bloquea_pero_se_reporta():
    """REVIEW no debe fallar el test (es una zona gris para revisión humana,
    no un bloqueo automático) pero el índice debe reflejar el problema real."""
    reference = load_csv_as_records(FIXTURES / "reference.csv")
    candidate = load_csv_as_records(FIXTURES / "candidate_review.csv")
    result = assert_parity(reference, candidate, config=CONFIG)  # no debe lanzar
    assert result["verdict"] == "REVIEW"
    assert 0.95 <= result["index"] < 0.99
    # la causa exacta debe ser rastreable: null_integrity es la metrica que baja
    assert result["metrics"]["null_integrity"]["score"] < 1.0
    assert result["metrics"]["row_count"]["score"] == 1.0
    assert result["metrics"]["aggregates"]["score"] == 1.0


def test_fail_lanza_assertionerror_con_detalle():
    """FAIL debe fallar el test de forma ruidosa -- así se engancha a CI, no a
    que alguien recuerde revisar un reporte manualmente."""
    reference = load_csv_as_records(FIXTURES / "reference.csv")
    candidate = load_csv_as_records(FIXTURES / "candidate_fail.csv")
    with pytest.raises(AssertionError, match="Paridad FAIL"):
        assert_parity(reference, candidate, config=CONFIG)


def test_metric_row_count_identico_da_score_1():
    stats_a = {"row_count": 100}
    stats_b = {"row_count": 100}
    assert metric_row_count(stats_a, stats_b)["score"] == 1.0


def test_metric_row_count_score_proporcional_a_la_diferencia():
    stats_a = {"row_count": 100}
    stats_b = {"row_count": 90}
    result = metric_row_count(stats_a, stats_b)
    assert result["score"] == pytest.approx(0.90)
    assert result["detail"]["diff_pct"] == pytest.approx(0.10)


def test_compare_sin_reglas_configuradas_da_score_perfecto():
    """Con metrics.aggregates.rules vacio, esa metrica no debe arrastrar el
    indice hacia abajo -- una metrica sin nada que comparar no es un fallo."""
    records_a = [{"id": 1}, {"id": 2}]
    records_b = [{"id": 1}, {"id": 2}]
    stats_a = compute_stats_from_records(records_a, {})
    stats_b = compute_stats_from_records(records_b, {})
    result = compare(stats_a, stats_b, {})
    assert result["verdict"] == "SUCCESS"


def test_aggregates_respeta_tolerancia_relativa():
    records_a = [{"monto": 1000.0}]
    records_b = [{"monto": 1000.5}]  # 0.05% de diferencia, dentro de numeric_rel=0.001
    config = {"metrics": {"aggregates": {"rules": [{"func": "sum", "column": "monto"}]}}}
    stats_a = compute_stats_from_records(records_a, config)
    stats_b = compute_stats_from_records(records_b, config)
    result = compare(stats_a, stats_b, config)
    assert result["metrics"]["aggregates"]["score"] == 1.0


def test_schema_types_detecta_columna_faltante():
    records_a = [{"id": 1, "monto": 10.0}]
    records_b = [{"id": 1}]  # falta 'monto'
    config = {"metrics": {"schema_types": {"enabled": True}}}
    stats_a = compute_stats_from_records(records_a, config)
    stats_b = compute_stats_from_records(records_b, config)
    result = compare(stats_a, stats_b, config)
    assert result["metrics"]["schema_types"]["score"] < 1.0
    assert "monto" in result["metrics"]["schema_types"]["detail"]["missing_on_either_side"]
