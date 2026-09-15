
# 10 — Paridad Alteryx ↔ PySpark (Fabric) — v0.2.0
📅 Fecha: 2025-10-30
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)

> Propósito: Definir cómo medir y reportar la equivalencia funcional entre el flujo Alteryx y su conversión a PySpark.
> Cambios v0.2.0: aclaraciones para joins no-inner y uso recomendado de `coalesce` antes de agregaciones numéricas.

---

## 1) Definición de paridad
La paridad se cumple cuando, para un mismo dataset de prueba, el resultado en PySpark coincide con el de Alteryx dentro de tolerancias predefinidas (conteos, agregados, esquema y nulos).

---

## 2) Configuración de métricas (YAML)
```yaml
parity_config:
  tolerances:
    numeric_abs: 0.0001
    numeric_rel: 0.001
  weights:
    row_count: 0.35
    aggregates: 0.35
    schema_types: 0.15
    null_integrity: 0.10
    value_domains: 0.05
  metrics:
    row_count: { enabled: true }
    aggregates:
      enabled: true
      rules:
        - { func: "sum", column: "monto" }
        - { func: "avg", column: "monto" }
    schema_types: { enabled: true }
    null_integrity:
      enabled: true
      required_cols: ["id"]
    value_domains: { enabled: false, columns: {} }
```

---

## 3) Consideraciones con joins `left/right/outer`
- Tras un join no-inner, es común que haya **NULLs** en columnas numéricas del lado sin correspondencia.
- **SUM** en Spark **ignora NULL**; no afecta el total. Aun así, para evitar propagación de nulos en cálculos posteriores, se recomienda aplicar `coalesce(col, 0.0)` **antes** de agregaciones de sumas.
- **AVG** ignora NULL (promedia solo no-nulos). **Reemplazar NULL por 0 cambia la semántica**. Solo aplicar coalesce a AVG si el negocio lo requiere explícitamente.
- Esta guía armoniza la comparación contra Alteryx cuando el diseñador esperaba preservar totales en filas sin match.

Ejemplo previo a agregaciones:
```python
from pyspark.sql import functions as F

df_eval = df_joined.withColumn("monto", F.coalesce(F.col("monto"), F.lit(0.0)))
res = df_eval.groupBy("segmento").agg(F.sum("monto").alias("monto_total"))
```

---

## 4) Snippets de comparación (resumen)
- `parity_row_count(df_a, df_b, ...)` — compara conteos.
- `parity_aggregates(df_a, df_b, rules, ...)` — compara sumas y promedios con tolerancias absoluta/relativa.
- `parity_schema_types(df_a, df_b, ...)` — detecta diferencias de tipos.
- `parity_null_integrity(df_a, df_b, required_cols, ...)` — compara nulos en columnas clave.
- `parity_index(results)` — cálculo del índice ponderado.

---

## 5) Criterios de éxito
- Índice ≥ **0.99** → ✅ SUCCESS
- 0.95–0.99 → ⚠️ REVIEW
- < 0.95 → ❌ FAIL

---

## 6) Notas de implementación
- Integrar con `07_prompt_contract.md` v0.2.0 (inferencias de Join) y `09_QA_checklist_y_GX_basico.md` (QA de conteos).
- Documentar cualquier aplicación de `coalesce` en el log con `WARN_COALESCE_APPLIED` o `INFO_COALESCE_APPLIED`.
