# Validación de paridad Alteryx ↔ PySpark

Define cómo medir si el notebook migrado (Fase 4) produce el mismo resultado
que el flujo Alteryx original, **como una prueba automatizada (`pytest`), no
como una revisión manual fila por fila**. Implementado en
`scripts/parity.py`; ejemplo runnable en `scripts/test_parity_example.py`.

## Idea central

1. Corres el flujo Alteryx original sobre un dataset de prueba y exportas el
   resultado (CSV/Parquet) — esa es la **referencia**.
2. Corres el notebook generado (Fase 4) sobre el mismo dataset de entrada y
   exportas su resultado — ese es el **candidato**.
3. `scripts/parity.py` compara ambos con 5 métricas ponderadas y calcula un
   **índice de paridad** entre 0 y 1.

| Índice | Veredicto | Significado |
|---|---|---|
| ≥ 0.99 | `SUCCESS` | Equivalente para efectos prácticos. |
| 0.95 – 0.99 | `REVIEW` | Diferencia real pero acotada — requiere que una persona la revise; el test **no falla** solo por estar en este rango. |
| < 0.95 | `FAIL` | El test lanza `AssertionError` con el detalle completo. |

## Las 5 métricas

| Métrica | Peso default | Qué compara |
|---|---|---|
| `row_count` | 0.35 | Conteo total de filas, como fracción de diferencia relativa. |
| `aggregates` | 0.35 | Reglas configuradas (`sum`/`avg`/`min`/`max`/`count`/`count_distinct` sobre columnas concretas), cada una dentro de tolerancia absoluta o relativa. |
| `schema_types` | 0.15 | Que ambos lados tengan las mismas columnas y el mismo tipo por columna. |
| `null_integrity` | 0.10 | Que la tasa de NULL en las columnas requeridas (`required_cols`) sea la misma en ambos lados — no el conteo absoluto, la tasa (para que no penalice solo por tener más o menos filas). |
| `value_domains` | 0.05 | Que la tasa de valores fuera de un dominio permitido (ej. `status` solo puede ser `A`/`B`/`C`) sea similar en ambos lados. Deshabilitada por default. |

Los pesos se **renormalizan entre las métricas habilitadas** — si deshabilitas
`value_domains` (el default), el resto no necesita sumar 1.0.

**Nota sobre granularidad**: `aggregates` y `schema_types` puntúan por
fracción de reglas/columnas que pasan, no de forma continua. Con pocas
reglas configuradas, un solo fallo pesa mucho (con 3 reglas, una que falla ya
cuesta 1/3 de esa métrica). Configura suficientes reglas de agregación
(sobre las columnas de negocio relevantes, no solo una) para que el índice
sea informativo y no binario.

## Tolerancias

```python
"tolerances": {"numeric_abs": 0.0001, "numeric_rel": 0.001}
```

Un valor pasa si `abs(a - b) <= numeric_abs` **o** `abs(a - b) <= numeric_rel * max(abs(a), abs(b), 1)`. Ajusta `numeric_rel` hacia arriba si el negocio tolera más
variación (ej. montos financieros redondeados a 2 decimales pueden necesitar
menos tolerancia que agregados de conteos grandes).

## Dos formas de calcular las estadísticas

- `compute_stats_from_records(records: list[dict], config)` — puro Python,
  sin Spark. Sirve para pilotos chicos: cargas dos CSV con
  `load_csv_as_records()` y comparas sin necesitar un cluster. Es lo que usa
  `test_parity_example.py`.
- `compute_stats_spark(df, config)` — requiere una sesión Spark activa
  (Fabric/Databricks). Agrega **en el motor** (nunca hace `.collect()` de las
  filas completas, solo de los resultados agregados) — es la que se usa en
  producción con datasets grandes.

**No mezcles las dos** al comparar: `compare(stats_a, stats_b, ...)` asume
que ambos vienen del mismo método, porque `schema_types` compara nombres de
tipo textualmente (`"int"` vs `"bigint"` de Spark no es lo mismo que `"int"`
de Python) y una comparación cruzada da falsos negativos.

## Cómo escribir el test de un workflow real

Copia el patrón de `scripts/test_parity_example.py`:

```python
from parity import assert_parity, load_csv_as_records

CONFIG = {
    "metrics": {
        "aggregates": {"rules": [
            {"func": "sum", "column": "monto"},
            {"func": "count", "column": "cliente_id"},
        ]},
        "null_integrity": {"required_cols": ["id", "cliente_id"]},
    }
}

def test_ventas_parity():
    reference = load_csv_as_records("referencia/ventas_alteryx.csv")
    candidate = load_csv_as_records("salida/ventas_spark.csv")
    result = assert_parity(reference, candidate, config=CONFIG)
    assert result["verdict"] != "FAIL"  # o == "SUCCESS" si quieres ser mas estricto
```

`assert_parity`/`assert_parity_spark` ya lanzan `AssertionError` en `FAIL` —
el `assert` explícito en el test es solo para dejar el criterio de éxito
legible en el propio archivo (y para poder exigir `SUCCESS` estricto en
workflows regulatorios en vez de tolerar `REVIEW`).

## Fixtures de referencia

`fixtures/parity/reference.csv` + 3 variantes (`candidate_success.csv`,
`candidate_review.csv`, `candidate_fail.csv`) calibradas para caer en cada
banda de veredicto — úsalas para probar cambios a `parity.py` antes de
confiar en él para un workflow real, igual que los fixtures `.yxmd` se usan
para probar el parser y el transpilador.
