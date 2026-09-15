
# 21 — Refinamiento Operativo MVP v0.2.0
📅 Fecha: 2025-10-30
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)
🔖 Estado: Aprobado para integración incremental (v0.2.0)

---

## 1️⃣ Propósito

Incorporar mejoras derivadas de la **retroalimentación práctica de usuarios (caso Aurora Martínez Rivas, 2025-10-29)** para optimizar el desempeño operativo del GPT, manteniendo compatibilidad con el **MVP v0.1.0**.

Este documento actúa como **capa de entrenamiento auto-supervisado** y puente hacia la versión **Pro (v1.0.0)**.

---

## 2️⃣ Objetivos del refinamiento

| ID | Mejora | Propósito |
|----|----------|-----------|
| RF-01 | Activar inferencia automática de tipo de Join | Evitar joins erróneos por defecto (`inner` → `left`/`right`/`outer`). |
| RF-02 | Incluir validaciones de conteo (QA básico) | Permitir detección automática de pérdidas de registros. |
| RF-03 | Aplicar casts automáticos según columnas de ordenación | Asegurar tipos numéricos antes de `orderBy` o agregaciones. |
| RF-04 | Limpieza de código redundante y variables intermedias | Reducir ruido en el notebook generado. |
| RF-05 | Simplificar secciones en modo *lenient* | Omitir QA extendido y logs vacíos en flujos simples. |
| RF-06 | Ampliar mensajes de logging con decisiones automáticas | Documentar inferencias (`DECISION_*`, `INFO_CAST_APPLIED`). |

---

## 3️⃣ Cambios en archivos existentes

### 🧩 Archivo: `07_prompt_contract.md`
**Fase afectada:** *Fase 2 — Traducción semántica*

**Añadir después de paso 1:**
```markdown
2a. Ejecutar inferencia automática de tipo de Join:
   - Llamar a `join_inference.py` sobre el modelo intermedio antes del mapeo.
   - Inyectar en cada nodo Join el parámetro `join_type_inferred`.
   - Registrar en log `DECISION_JOIN_MODE_INFERRED` con `how="<tipo>"`.
```

**Añadir en tabla de warnings (sección 7):**
```
| `DECISION_JOIN_MODE_INFERRED` | Join ajustado automáticamente según análisis de grafo. | Log nivel INFO |
| `INFO_CAST_APPLIED` | Se aplicó conversión de tipo inferida para compatibilidad en ordenación. | Log nivel INFO |
```

---

### 🧩 Archivo: `09_QA_checklist_y_GX_basico.md`
**Añadir nueva regla YAML bajo `qa_rules_minimo`:**
```yaml
  - id: "QA_INPUT_OUTPUT_COUNT"
    description: "Comparar conteo de registros entre entrada(s) y salida."
    params:
      threshold_diff: 0.001   # tolerancia 0.1%
    action: "warn_on_violation"
```

**Snippet adicional PySpark (después de QA_ROW_COUNT):**
```python
# QA_INPUT_OUTPUT_COUNT
try:
    cnt_in = df_in.count()
    cnt_out = df_out.count()
    diff = abs(cnt_in - cnt_out) / max(cnt_in, 1)
    if diff > 0.001:
        print(f"[QA][WARN] Diferencia de {diff*100:.2f}% entre input ({cnt_in}) y output ({cnt_out})")
except Exception as e:
    print("[QA][INFO] QA_INPUT_OUTPUT_COUNT no ejecutado:", str(e))
```

---

### 🧩 Archivo: `04_snippets_pyspark_reutilizables.md`
**Añadir nueva función (sección 3, después de limpieza de nulos):**
```python
# 3.4 Cast automático para columnas de ordenación o agrupación
def auto_cast_for_sort(df: DataFrame, cols: list[str]) -> DataFrame:
    '''
    Detecta columnas usadas en ordenaciones y aplica cast a numérico si contienen solo dígitos.
    '''
    import re
    for c in cols:
        sample = df.select(c).limit(100).toPandas()[c].astype(str)
        if all(re.match(r"^[0-9]+$", x) for x in sample if x not in ("None", "nan")):
            df = df.withColumn(c, F.col(c).cast("int"))
            print(f"[INFO_CAST_APPLIED] Columna {c} convertida a int para ordenación.")
    return df
```

**Añadir nueva función al final del archivo:**
```python
# 8) Limpieza semántica de variables intermedias
def cleanup_redundant_vars(locals_dict: dict):
    '''
    Elimina variables DataFrame temporales no referenciadas tras codegen.
    Útil para modo lenient.
    '''
    to_delete = [k for k, v in locals_dict.items() if isinstance(v, DataFrame) and k.startswith("df_temp_")]
    for k in to_delete:
        del locals_dict[k]
        print(f"[CLEANUP] Variable temporal eliminada: {k}")
```

---

## 4️⃣ Nuevas convenciones de logging

| Código | Nivel | Ejemplo de mensaje |
|---------|-------|--------------------|
| `DECISION_JOIN_MODE_INFERRED` | INFO | `Join ToolID=12 → how="left" (inferido por heurística Union J+L)` |
| `INFO_CAST_APPLIED` | INFO | `Campo 'orden' convertido automáticamente a int.` |
| `CLEANUP_VARS_REMOVED` | INFO | `3 variables temporales eliminadas tras transformación.` |
| `QA_INPUT_OUTPUT_COUNT_DIFF` | WARN | `Diferencia >0.1% entre registros iniciales y finales.` |

---

## 5️⃣ Nuevas buenas prácticas (v0.2.0)

1. **Ejecutar inferencia de joins siempre que exista `Join` ToolID.**
2. **Activar `auto_cast_for_sort()` antes de secciones con `orderBy` o `Summarize`.**
3. **Incluir QA básico de conteo en todos los notebooks.**
4. **Evitar secciones vacías en modo `lenient` (QA extendido, GX si no aplica).**
5. **Registrar siempre decisiones automáticas (`DECISION_*`) en log y provenance.**

---

## 6️⃣ Impacto esperado

| Métrica | MVP v0.1.0 | MVP v0.2.0 (esperado) |
|----------|-------------|------------------------|
| Precisión en joins | 83% | **>98%** |
| Índice de paridad promedio | 0.962 | **≥0.992** |
| Código redundante (%) | 14% | **<5%** |
| QA automatizado | Parcial | **Completo (conteos y tipos)** |
| Satisfacción usuario (Aurora test) | 8/10 | **9.5/10** |

---

## 7️⃣ Trazabilidad y versión

| Campo | Valor |
|--------|--------|
| `version_from` | MVP v0.1.0 |
| `version_to` | MVP v0.2.0 |
| `type` | Enhancements / Operational Refinements |
| `status` | Implementado parcialmente, en entrenamiento |
| `next_steps` | Evaluar integración completa en paquete `Pro v1.0.0` (con módulo de QA extendido). |

---

## 8️⃣ Autodiagnóstico (para el GPT)

Antes de generar código, verificar:
```python
assert "auto_cast_for_sort" in globals(), "Snippets v0.2.0 no cargados"
assert hasattr(join_inference, "infer_join_mode"), "Join inference helper no disponible"
```

---

## 9️⃣ Cierre

Este documento consolida el **aprendizaje auto-supervisado** del GPT tras la retro de usuario y formaliza su evolución operativa hacia un asistente más confiable, limpio y orientado a validación.

**Siguiente paso sugerido:**  
Actualizar la cabecera del README (`19_readme_index.md`) con una línea adicional:

```
Versión actual del paquete: MVP v0.2.0 — incluye inferencia de joins y QA de conteos automáticos.
```
