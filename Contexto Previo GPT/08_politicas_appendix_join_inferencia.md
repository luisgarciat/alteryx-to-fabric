
# 08 — Políticas de Conversión (Join con salidas J/L/R) — v0.2.0
📅 Fecha: 2025-10-30
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)
🔖 Estado: Actualizado con nuevos códigos `DECISION_*` y `INFO_CAST_APPLIED`

---

## 1️⃣ Propósito
Definir las políticas operativas para la inferencia de joins y logging asociado en el proceso de conversión de flujos Alteryx → PySpark.

---

## 2️⃣ Nuevos códigos de Warning / Info / Decision

| Código | Nivel | Descripción | Ejemplo |
|---------|-------|-------------|----------|
| `INFO_JOIN_OUTPUTS_INFERRED` | INFO | Se infirieron salidas del Join por análisis del grafo. | Join ToolID=4 inferido left |
| `WARN_INCOMPLETE_JOIN_OUTPUTS` | WARN | XML sin salidas completas; heurística aplicada. | ToolID=4 inner asumido |
| `WARN_UNION_AMBIGUO_POST_JOIN` | WARN | Se detectó Union tras Join sin trazas claras. | Revisar manualmente. |
| `WARN_COALESCE_APPLIED` | WARN | Se aplicó `coalesce` para preservar métricas numéricas. | |
| **`DECISION_JOIN_MODE_INFERRED`** | INFO | Join ajustado automáticamente según heurística de inferencia. | Join ToolID=12 → how="left" |
| **`INFO_CAST_APPLIED`** | INFO | Cast automático aplicado en columna detectada como numérica. | Columna `orden` → int |
| **`CLEANUP_VARS_REMOVED`** | INFO | Variables temporales eliminadas tras codegen. | count=3 |
| **`QA_INPUT_OUTPUT_COUNT_DIFF`** | WARN | Diferencia >0.1% entre input/output. | 0.87% |

---

## 3️⃣ Política de modo operativo

| Modo | Descripción | Acción ante ambigüedad |
|------|--------------|------------------------|
| **strict** | Falla si no se puede determinar inequívocamente la salida (`J/L/R`). | Genera `ERR_JOIN_OUTPUT_UNDETERMINED` |
| **lenient** (por defecto) | Aplica reglas de inferencia y continúa. | Usa `inner` si persiste ambigüedad, registra `WARN_INCOMPLETE_JOIN_OUTPUTS` |

---

## 4️⃣ Logging estándar

Formato:  
```
timestamp | nivel | ToolID | código | mensaje
```

Ejemplos:
```
2025-10-30T10:12:22Z | INFO | 12 | DECISION_JOIN_MODE_INFERRED | how=left via heur_union
2025-10-30T10:12:23Z | INFO | 15 | INFO_CAST_APPLIED | columna=orden type=int
2025-10-30T10:12:23Z | INFO | -  | CLEANUP_VARS_REMOVED | count=3
2025-10-30T10:12:24Z | WARN | 13 | QA_INPUT_OUTPUT_COUNT_DIFF | pct=0.87
```

---

## 5️⃣ Relación con Join Inference

- La inferencia se realiza antes del mapeo de PySpark (`prompt_contract.md`, fase 2).  
- Fuente de verdad: `join_inference.py`  
- El resultado debe incluir:
  ```json
  {
    "join_type": "left",
    "inferred": true,
    "rule": "heur_union_multi_inputs"
  }
  ```

---

## 6️⃣ Políticas adicionales

- Si el join es `left/right/outer`, aplicar `coalesce` en métricas numéricas antes de agregaciones.  
- En caso de Join → Union → Summarize, mantener `join_type` del primer Join.  
- Registrar cada decisión automática como `# DECISION:` en el código PySpark generado.

---

## 7️⃣ Ejemplo de trazabilidad en código generado

```python
# TOOL: Join
# SOURCE: ToolID=12
# DECISION: how="left" (inferido por reglas heur_union_multi_inputs)
# INFO: union_tool_id=4
df_join = safe_join(df_a, df_b, ["id"], how="left")
```

---

## 8️⃣ Trazabilidad en reportes

| Campo | Descripción |
|--------|--------------|
| `ToolID` | Identificador del nodo Join analizado. |
| `join_type_inferred` | true/false. |
| `rule_applied` | Nombre de la regla usada (`explicit_JL`, `heur_union_multi_inputs`, etc.). |
| `union_tool_id` | ID de herramienta Union usada como evidencia (si aplica). |
| `warnings` | Lista de códigos `WARN_*` generados. |

---

## 9️⃣ Compatibilidad

- Compatible con `prompt_contract.md` v0.2.0.  
- Requiere `join_inference.py` actualizado.  
- Logging integrado en `report.html` y `conversion.log`.

**Versión:** MVP v0.2.0  
**Compatibilidad:** Fabric Runtime 1.3+  
**Autor:** GPT de Migración Alteryx → PySpark (Fabric)
