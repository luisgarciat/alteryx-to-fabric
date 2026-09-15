
# Addendum — Mapeo Join Alteryx → PySpark con inferencia automática (v0.2.0)
📅 Fecha: 2025-10-30

## Objetivo
Detectar automáticamente la intención de salida del Join de Alteryx (J, L, R) y mapearlo a `how` en Spark.

## Mapeo (equivalencias)
- **J** → `inner`
- **J ∪ L** → `left`
- **J ∪ R** → `right`
- **J ∪ L ∪ R** → `outer` *(sinónimos: `full`, `full_outer`, `outer` en Spark)*

## Reglas de inferencia (resumen)
1. **Explícito por outputs**: si el XML lista `J/L/R`, usar mapeo directo.
2. **Heurística por Union aguas abajo**: si un `Union` combina varias salidas del mismo Join, inferir `left/right/outer` según combinación observada.
3. **Fallback**: si no hay señal suficiente → `inner` y registrar `WARN_INCOMPLETE_JOIN_OUTPUTS` (o error en modo `strict`).

## Trazabilidad y contratos
- Registrar decisión con `DECISION_JOIN_MODE_INFERRED` en `conversion.log`.
- Inyectar comentario en código: `# DECISION: how="<tipo>" (regla=<...>)`.
- Ver `07_prompt_contract.md` v0.2.0 para el orden de ejecución (inferir antes de mapear).

## Nota de métricas
Para preservar sumatorias cuando hay `NULL` del lado sin match, considerar `coalesce` antes de agregaciones numéricas; ver `10_paridad_alteryx_pyspark.md` v0.2.0.
