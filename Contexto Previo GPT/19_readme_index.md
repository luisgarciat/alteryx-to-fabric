
# 📘 GPT de Migración Alteryx → PySpark (Fabric)
**Versión actual:** MVP v0.2.0  
📅 Fecha: 2025-10-30  
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)

---

## 1️⃣ Propósito del Paquete

Este paquete define la arquitectura, plantillas, políticas y snippets reutilizables que permiten la **conversión automatizada de flujos Alteryx (.yxzp/.yxmd) a pipelines PySpark ejecutables en Microsoft Fabric**.

---

## 2️⃣ Novedades — v0.2.0

> **Feedback base:** Caso práctico “Aurora Martínez Rivas” (2025-10-29)

| ID | Mejora | Archivo principal | Descripción |
|----|---------|------------------|--------------|
| RF-01 | Inferencia automática de tipo de Join | `07_prompt_contract.md`, `08_politicas_appendix_join_inferencia.md` | Detecta y ajusta automáticamente el tipo de join (`inner/left/right/outer`). |
| RF-02 | QA de conteos (input/output) | `09_QA_checklist_y_GX_basico.md` | Nuevo control `QA_INPUT_OUTPUT_COUNT` para detectar pérdidas de registros. |
| RF-03 | Cast automático en ordenaciones | `04_snippets_pyspark_reutilizables.md` | Nueva función `auto_cast_for_sort()` para columnas numéricas. |
| RF-04 | Limpieza semántica post-codegen | `04_snippets_pyspark_reutilizables.md`, `05_template_notebook_fabric.ipynb` | Nueva función `cleanup_redundant_vars()` y bloque automático de limpieza. |
| RF-05 | Logging extendido y decisiones documentadas | `08_politicas_appendix_join_inferencia.md`, `07_prompt_contract.md` | Nuevos códigos: `DECISION_JOIN_MODE_INFERRED`, `INFO_CAST_APPLIED`, `QA_INPUT_OUTPUT_COUNT_DIFF`. |
| RF-06 | Documento de refinamiento | `21_refinamiento_operativo_mvp_v0.2.0.md` | Nuevo documento que centraliza el entrenamiento auto-supervisado del GPT. |

---

## 3️⃣ Requisitos de versión

| Componente | Versión mínima | Estado |
|-------------|----------------|--------|
| Microsoft Fabric Runtime | 1.3+ | ✅ Compatible |
| PySpark | 3.4+ | ✅ Compatible |
| Great Expectations (opcional) | 0.17+ | ✅ Soportado (QA extendido) |

---

## 4️⃣ Estructura de Archivos (actualizada)

| ID | Archivo | Propósito | Versión |
|----|----------|-----------|----------|
| 01 | `01_modelo_intermedio_schema.json` | Esquema base del modelo intermedio | v0.1.0 |
| 02 | `02_modelo_intermedio_ejemplos.json` | Ejemplos de referencia | v0.1.0 |
| 03 | `03_mapeo_addendum_join_inferencia.md` | Reglas heurísticas Join/Union | v0.1.0 |
| 04 | `04_snippets_pyspark_reutilizables.md` | Snippets reutilizables (lectura, join, QA, cleanup) | **v0.2.0** |
| 05 | `05_template_notebook_fabric.ipynb` | Notebook base para ejecución en Fabric | **v0.2.0 (parche sugerido)** |
| 06 | `06_template_parameters.yaml` | Parámetros de notebook | v0.1.0 |
| 07 | `07_prompt_contract.md` | Contrato operativo del GPT | **v0.2.0** |
| 08 | `08_politicas_appendix_join_inferencia.md` | Políticas y logging | **v0.2.0** |
| 09 | `09_QA_checklist_y_GX_basico.md` | QA mínimo y GX base | **v0.2.0** |
| 10 | `10_paridad_alteryx_pyspark.md` | Medición de equivalencia Alteryx ↔ PySpark | v0.1.0 |
| 19 | `19_readme_index.md` | Índice y guía general | **v0.2.0** |
| 21 | `21_refinamiento_operativo_mvp_v0.2.0.md` | Documento de refinamiento y entrenamiento | **Nuevo (v0.2.0)** |

---

## 5️⃣ Quickstart

```bash
# Ejemplo de ejecución
gpt_migracion.run(
    workflow="Ventas_Q4.yxzp",
    lakehouse_name="VentasLH",
    path_salida="Files/Migracion/",
    modo="lenient",
    usuario="Aurora M.R.",
    fecha_ejecucion="2025-10-30"
)
```

El GPT generará automáticamente:

- `notebook_pyspark.ipynb` listo para ejecución en Fabric.  
- `report.html` con advertencias y métricas QA.  
- `conversion.log` con trazas de inferencia y decisiones (`DECISION_*`).

---

## 6️⃣ Ejemplo de log (v0.2.0)

```
2025-10-30T10:42:11Z | INFO | ToolID=12 | DECISION_JOIN_MODE_INFERRED how=left via heur_union
2025-10-30T10:42:11Z | INFO | ToolID=15 | INFO_CAST_APPLIED columna=orden
2025-10-30T10:42:12Z | WARN | ToolID=13 | QA_INPUT_OUTPUT_COUNT_DIFF pct=0.83
```

---

## 7️⃣ Changelog

| Versión | Fecha | Resumen |
|----------|--------|----------|
| v0.1.0 | 2025-10-24 | MVP inicial con parser y codegen base. |
| **v0.2.0** | **2025-10-30** | Integración de inferencia de joins, QA de conteos, cast automático y logging extendido. |

---

## 8️⃣ Próximos pasos (hacia Pro v1.0.0)

- QA extendido configurable por YAML (`qa_extended=true`).  
- Métricas de paridad visualizadas en `report.html`.  
- Integración con *Great Expectations* en Fabric nativo.  
- Testing automático de snippets (`pytest + spark-testing-base`).

---

**Autor:** GPT de Migración Alteryx → PySpark (Fabric)  
**Contacto:** equipo_migracion@datatech.internal  
**Licencia:** uso interno corporativo.
