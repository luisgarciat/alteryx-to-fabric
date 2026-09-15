
# 09 — QA Checklist y Great Expectations (GX) Básico — v0.2.0
📅 Fecha: 2025-10-30
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)
🔖 Estado: Actualizado con mejoras MVP v0.2.0 (QA de conteos y mensajes extendidos)

> **Propósito:** Definir validaciones automáticas de calidad de datos para notebooks PySpark en Microsoft Fabric (mínimas y extendidas), y una configuración base de **Great Expectations (GX)** lista para insertar por el generador (codegen).  
> **Ámbito:** Se ejecuta tras `TRANSFORMS` y antes/durante `OUTPUTS` en el notebook estándar.

---

## 1️⃣ Checklist QA mínimo (sin GX)
Validaciones rápidas que no requieren librerías externas. Deben ejecutarse **siempre** (MVP).

- **Conteo de filas de salida** (sanity check).  
- **Comparación de conteo input-output** (nuevo en v0.2.0).  
- **Null-checks** en columnas clave/obligatorias (si existen).  
- **Verificación de tipos** (casts explícitos realizados con éxito).  
- **Rangos básicos numéricos** (si aplica: montos ≥ 0).  
- **Formato de fechas** cuando se usó `to_date/to_timestamp` con formato explícito.  

```yaml
qa_rules_minimo:
  - id: "QA_ROW_COUNT"
    description: "Contar filas resultantes."
    action: "log_only"

  - id: "QA_INPUT_OUTPUT_COUNT"
    description: "Comparar conteo de registros entre entrada(s) y salida."
    params:
      threshold_diff: 0.001   # tolerancia 0.1%
    action: "warn_on_violation"

  - id: "QA_NOT_NULL_KEYS"
    description: "Columnas clave no deben contener NULL."
    params:
      required_cols: ["id"]
    action: "fail_on_violation_if_strict"

  - id: "QA_TYPES_CAST"
    description: "Verificar éxito de casts explícitos."
    params:
      casts_expected: {}
    action: "warn_on_violation"

  - id: "QA_NUMERIC_RANGE"
    description: "Validar montos >= 0 (o regla definida)."
    params:
      rules:
        - column: "monto"
          condition: ">= 0"
    action: "warn_on_violation"

  - id: "QA_DATE_FORMAT"
    description: "Fechas válidas tras to_date/to_timestamp."
    params:
      columns: ["fecha_dt"]
    action: "warn_on_violation"
```

**Snippet PySpark mínimo (plantilla):**
```python
# QA_ROW_COUNT
row_count = df_out.count()
print("[QA] Row count:", row_count)

# QA_INPUT_OUTPUT_COUNT (nuevo en v0.2.0)
try:
    cnt_in = df_in.count()
    cnt_out = df_out.count()
    diff = abs(cnt_in - cnt_out) / max(cnt_in, 1)
    if diff > 0.001:
        print(f"[QA][WARN] Diferencia de {diff*100:.2f}% entre input ({cnt_in}) y output ({cnt_out})")
except Exception as e:
    print("[QA][INFO] QA_INPUT_OUTPUT_COUNT no ejecutado:", str(e))

# QA_NOT_NULL_KEYS
required_cols = params.get("required_cols", [])
for c in required_cols:
    nulls = df_out.filter(F.col(c).isNull()).count()
    if nulls > 0:
        print(f"[QA][WARN] NULLs en {c}: {nulls}")

# QA_TYPES_CAST (si el generador definió casts_expected)
casts_expected = params.get("casts_expected", {})
for col_name, target_type in casts_expected.items():
    actual = dict(df_out.dtypes).get(col_name)
    if actual != target_type:
        print(f"[QA][WARN] Cast no aplicado en {col_name}. Actual={actual}, Esperado={target_type}")

# QA_NUMERIC_RANGE
for r in params.get("rules", []):
    col, cond = r["column"], r["condition"]
    if cond.strip() == ">= 0":
        negatives = df_out.filter(F.col(col) < 0).count()
        if negatives > 0:
            print(f"[QA][WARN] Valores negativos en {col}: {negatives}")

# QA_DATE_FORMAT
for c in params.get("columns", []):
    invalid = df_out.filter(F.col(c).isNull()).count()
    if invalid > 0:
        print(f"[QA][WARN] Formato de fecha inválido en {c}: {invalid}")
```

---

## 2️⃣ Great Expectations (GX) — Configuración base

Sección preparada para que el generador inicialice GX y ejecute **expectations** mínimas sobre `df_out`.

```yaml
gx_expectations_base:
  datasource:
    name: "spark_df_runtime"
    type: "runtime"
  checkpoint:
    name: "checkpoint_mvp"
    expectations:
      - type: "expect_table_row_count_to_be_greater_than"
        params: { value: 0 }
      - type: "expect_column_values_to_not_be_null"
        params: { column: "id" }
      - type: "expect_column_values_to_be_between"
        params: { column: "monto", min_value: 0, max_value: null, strict_min: true }
      - type: "expect_column_values_to_match_regex"
        params: { column: "email", regex: "^[^@]+@[^@]+\.[^@]+$" }
      - type: "expect_column_values_to_be_in_type_list"
        params: { column: "fecha_dt", type_list: ["date", "timestamp"] }
  data_docs: true
  on_failure: "warn"
```

**Snippet de setup GX (plantilla):**
```python
try:
    import great_expectations as ge
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.checkpoint import SimpleCheckpoint

    context = ge.get_context()
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_df_runtime",
        data_connector_name="runtime_data_connector",
        data_asset_name="df_out_asset",
        runtime_parameters={"batch_data": df_out},
        batch_identifiers={"default_identifier_name": "default_id"},
    )

    suite = context.create_expectation_suite("suite_mvp", overwrite_existing=True)
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    validator.expect_table_row_count_to_be_greater_than(value=0)

    checkpoint = SimpleCheckpoint(
        name="checkpoint_mvp",
        data_context=context,
        validations=[{"batch_request": batch_request, "expectation_suite_name": "suite_mvp"}],
    )
    result = checkpoint.run()
    print("[GX] Success:", result["success"] if isinstance(result, dict) and "success" in result else "Unknown")

except Exception as e:
    print("[GX][INFO] GX no inicializado o no disponible en este entorno:", str(e))
```

---

## 3️⃣ QA extendido (opcional)

```yaml
qa_extended:
  - id: "QA_DUPLICATES"
    description: "Detectar duplicados por claves lógicas."
    params:
      keys: ["id"]
    action: "warn_on_violation"

  - id: "QA_VALUE_DOMAINS"
    description: "Validar dominios de valores (enums o catálogos)."
    params:
      columns:
        status: ["A", "B", "C"]
    action: "warn_on_violation"

  - id: "QA_OUTLIERS"
    description: "Revisión rápida de outliers (p.ej., 3*IQR)."
    params:
      columns: ["monto"]
    action: "log_only"
```

---

## 4️⃣ Nuevos mensajes y convenciones (v0.2.0)

| Código | Nivel | Ejemplo |
|---------|-------|----------|
| `QA_INPUT_OUTPUT_COUNT_DIFF` | WARN | Diferencia >0.1% entre registros iniciales y finales. |
| `QA_CAST_TYPE_MISMATCH` | WARN | Cast no aplicado según esquema esperado. |
| `QA_DATE_FORMAT_INVALID` | WARN | Fechas no válidas en columna `fecha_dt`. |

---

## 5️⃣ Cierre

Este archivo actualizado incorpora la lógica de **comparación de conteo input-output**, la validación de tipos de cast y la extensión de mensajes de QA.  
Su objetivo es fortalecer la trazabilidad del flujo migrado y reducir errores silenciosos de pérdida o transformación de datos.

**Versión:** MVP v0.2.0  
**Compatibilidad:** Total con plantillas y mapeos v0.1.0–v0.2.0  
**Autor:** GPT de Migración Alteryx → PySpark (Fabric)
