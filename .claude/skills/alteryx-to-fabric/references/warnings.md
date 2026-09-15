# Catálogo de códigos WARN_* / ERR_* / INFO_* / DECISION_* / QA_*

Estos códigos los emite el **generador** (código determinista), no el modelo.
El modelo los usa para saber qué señalar al usuario y cómo redactar el reporte
de conversión — no debe inventar código nuevo ni renombrar estos.

Formato de log estándar:
```
timestamp | nivel | ToolID | código | mensaje
```

Consolidado de `07_prompt_contract.md`, `08_politicas_appendix_join_inferencia.md`,
`09_QA_checklist_y_GX_basico.md`, `10_paridad_alteryx_pyspark.md` y
`21_refinamiento_operativo_mvp_v0.2.0.md` del paquete GPT original, más
`WARN_ORDER_UNDEFINED` (nuevo, ver más abajo — citado por `SKILL.md` pero
nunca definido en el paquete original) y los códigos que emite
`scripts/formula_transpiler.py` (Fase 3, ver la sección dedicada más abajo).

## Orden de registro — el más importante

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `WARN_ORDER_UNDEFINED` | WARN (bloquea en `strict`) | Un Multi-Row Formula, Record ID, Running Total o Sample no tiene una columna de orden explícita en el modelo intermedio. Alteryx preserva el orden físico de registro; Spark no garantiza ningún orden salvo que se imponga con `orderBy`/`Window(...).orderBy(...)`. | `strict`: detener la conversión de ese nodo y pedir al usuario que identifique la columna de orden (o confirmar que el resultado es no-determinista y aceptable). `lenient`: usar el orden de una columna candidata si existe una clave obviamente secuencial (ej. un ID incremental), documentar la suposición como `WARN_ORDER_UNDEFINED`, y nunca usar `monotonically_increasing_id()` como sustituto de orden real. |

## Inferencia de Join

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `INFO_JOIN_OUTPUTS_INFERRED` | INFO | Se determinaron las salidas del Join (`J`/`L`/`R`) por análisis del grafo de conexiones. | Ninguna — informativo. |
| `DECISION_JOIN_MODE_INFERRED` | INFO | El `how` del join en Spark se fijó automáticamente por heurística (no vino explícito). | Insertar comentario `# DECISION: how="<tipo>" (regla=<...>)` en el código generado. |
| `WARN_INCOMPLETE_JOIN_OUTPUTS` | WARN | El XML no listaba salidas completas del Join; se aplicó heurística parcial. | Revisar manualmente si el `how` inferido es correcto. |
| `WARN_UNION_AMBIGUO_POST_JOIN` | WARN | Un Union recibe múltiples entradas del mismo Join pero no se puede determinar con certeza qué combinación de `J/L/R` representa. | Revisión manual obligatoria antes de aceptar el `how` asumido. |
| `ERR_JOIN_OUTPUT_UNDETERMINED` | ERROR (solo en `strict`) | Ninguna regla de inferencia (explícita ni heurística) pudo determinar el `how`. | `strict`: abortar la conversión de ese nodo. `lenient`: asumir `inner` y degradar a `WARN_INCOMPLETE_JOIN_OUTPUTS`. |

## Casts y tipos

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `INFO_CAST_APPLIED` | INFO | `auto_cast_for_sort` convirtió una columna a numérico antes de un `orderBy`/agregación. | Ninguna — informativo, pero queda trazado en el log. |
| `WARN_DATA_TYPE_CAST` | WARN | Se intentó un cast automático y no se pudo aplicar con éxito. | Revisar manualmente la columna; puede requerir limpieza previa (valores no numéricos mezclados). |
| `QA_CAST_TYPE_MISMATCH` | WARN | El tipo resultante de una columna no coincide con el tipo esperado configurado en `casts_expected`. | Revisar la transformación previa a esa columna. |

## Paridad y agregaciones

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `INFO_COALESCE_APPLIED` | INFO | Se aplicó `coalesce(col, 0)` antes de una suma para preservar totales tras un join no-inner. | Ninguna — comportamiento esperado y documentado en `spark_patterns.md`. |
| `WARN_COALESCE_APPLIED` | WARN | Se aplicó `coalesce` en un contexto donde podría cambiar la semántica (p. ej. antes de un `AVG`), no solo antes de un `SUM`. | Confirmar con el dueño del flujo si el reemplazo de NULL por 0 es correcto para esa métrica. |

## QA de conteos y validaciones

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `QA_INPUT_OUTPUT_COUNT_DIFF` | WARN | Diferencia mayor a la tolerancia configurada (`threshold_diff`, default 0.1%) entre registros de entrada y salida. | Investigar pérdida o duplicación de registros — no ignorar aunque el índice de paridad global sea alto. |
| `QA_DATE_FORMAT_INVALID` | WARN | Una columna de fecha quedó NULL tras `to_date`/`to_timestamp` con el formato configurado. | Revisar el patrón de formato contra el dato real (ver la nota de tokens `.NET` vs `java.time` en `formula_language.md`). |

## Limpieza de código generado

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `CLEANUP_VARS_REMOVED` | INFO | Se eliminaron variables DataFrame temporales sin referencias posteriores. | Ninguna — reduce ruido en modo `lenient`. |

## Transpilador de expresiones Formula (Fase 3)

Emitidos por `scripts/formula_transpiler.py`. Van adjuntos a la expresión
puntual (`params.expressions[i].warnings` o `params.warnings`) y también se
agregan a `metadata.warnings` del IR con el `ToolID` del nodo, vía
`scripts/transpile_formula.py`.

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `WARN_PARSE_ERROR` | WARN (bloquea en `strict`) | La expresión no es Formula-language válido según la gramática soportada (paréntesis sin cerrar, token inesperado, etc.). | Revisar la expresión original a mano; puede ser sintaxis rara de una versión vieja de Alteryx que el tokenizer no cubre todavía. |
| `WARN_UNSUPPORTED_FUNCTION` | WARN | La expresión usa una función de Alteryx que no está en `_FUNCTION_HANDLERS` (no está en `formula_language.md` tampoco). | El código generado queda como `_unsupported_function("Nombre", [...])` — un marcador que no ejecuta, para que no se traduzca en silencio. Añadir la función a `formula_language.md` y a `_FUNCTION_HANDLERS` si aparece con frecuencia en `expressions.txt`. |
| `WARN_UNSUPPORTED_EXPRESSION` | WARN | El AST produjo un tipo de nodo sin generador de código (no debería ocurrir con la gramática actual; indica un bug del transpilador). | Revisar `formula_transpiler.py` — es un hueco en el propio compilador, no en la expresión del usuario. |
| `WARN_AMBIGUOUS_PLUS` | WARN | Un operador `+` entre dos operandos sin tipo determinable en tiempo de generación (ninguno es string/número literal ni el resultado de una función con tipo de retorno conocido). Se generó como suma aritmética por default. | Confirmar el tipo real de los campos involucrados (Select/Input del origen) — si al menos uno es texto, cambiar a `F.concat` a mano. |
| `WARN_NON_LITERAL_ARG` | WARN | Un argumento que debía ser un literal de texto (patrón regex, carácter de padding, nombre de unidad) es en cambio una expresión — no se puede resolver en tiempo de generación. | Revisar el argumento; si depende de un valor en tiempo de ejecución, la función correspondiente necesita una reescritura manual, no una traducción automática. |
| `WARN_ARITY_MISMATCH` | WARN | Una función se llamó con un número de argumentos distinto al esperado (ej. `IIF` con más o menos de 3). | Revisar la expresión original; probablemente hay un error de captura en el parser de Alteryx o la expresión en sí está mal formada. |
| `WARN_ROW_REF_WITHOUT_WINDOW` | WARN (bloquea en `strict`) | Una expresión usa `[Row-n:Campo]`/`[Row+n:Campo]` pero se transpiló sin pasar `window_var` (es decir, fuera de un nodo Multi-Row Formula reconocido). | Confirmar que el nodo es efectivamente un Multi-Row Formula y que `transpile_formula.py` lo está transpilando con `window_var=f"w_{ToolID}"` — si el warning persiste, es un caso que el CLI no está detectando. |
| `WARN_UNSUPPORTED_DATE_TOKEN` | WARN | Un formato de fecha (`DateTimeParse`/`DateTimeFormat`) usa un token `strftime` que no está en la tabla de conversión de `formula_language.md`. | Añadir el token a la tabla y a `_DATE_TOKEN_MAP` en `formula_transpiler.py` si es de uso frecuente; si es un caso aislado, traducirlo a mano en el notebook generado. |
| `WARN_UNSUPPORTED_DATE_UNIT` | WARN | `DateTimeAdd`/`DateTimeDiff` con una unidad que no es un literal `"days"`/`"months"`/`"years"` reconocible. | Revisar la expresión — la unidad puede venir de una variable o de un valor no soportado (`"hours"`, `"weeks"`, etc. no están mapeados todavía). |
| `INFO_DATEDIFF_APPROX` | INFO | `DateTimeDiff` en meses o años se tradujo con `F.months_between`, que es una aproximación (no un conteo calendario exacto). | Ninguna acción obligatoria — documentar si el negocio necesita precisión calendario exacta en vez de la aproximación de Spark. |

## Generación de notebook (Fase 4)

Emitidos por `scripts/generate_notebook.py` al recorrer el grafo del IR.

| Código | Nivel | Causa | Acción |
|---|---|---|---|
| `ERR_UNSUPPORTED_TOOL` | ERROR (bloquea en `strict`) | El nodo es de un tipo sin handler de codegen (no está en `TRANSFORM_HANDLERS`) — incluye `Multi-Field Formula` hasta que tenga extractor en la Fase 2, y cualquier herramienta de `tool_mapping.md` que aún no se implementó. | `lenient`: genera `df_<id> = df_<upstream>` (passthrough) con `# TODO` y sigue. `strict`: no escribe el notebook — hay que ampliar el generador o convertir esa herramienta a mano. |
| `ERR_MACRO_REQUIRES_DECISION` | ERROR (bloquea en `strict`) | El nodo es una Macro custom — nunca se traduce automáticamente (ver `tool_mapping.md`). | Igual que `ERR_UNSUPPORTED_TOOL`: passthrough + `# TODO` en `lenient`, bloqueo en `strict`. Requiere decisión humana sobre la macro (reimplementar, sustituir, descartar). |
| `WARN_JOIN_KEYS_INCOMPLETE` | WARN (bloquea en `strict`) | Un nodo Join no tiene `join_keys.left`/`join_keys.right` completas y del mismo tamaño (XML sin `JoinInfo` reconocible). | Revisar `params._raw` del nodo Join manualmente y completar las llaves antes de generar en `strict`. |
| `WARN_JOIN_RIGHT_ANTI_UNSUPPORTED` | WARN | Solo la salida `Right` de un Join está conectada aguas abajo (sin `Join` ni `Left`) — equivale a un "right anti join", que Spark no expone directamente. | Se genera `inner` como fallback marcado; para el semantic correcto, invertir los lados y usar `how="left_anti"` a mano. |
| `WARN_UNSUPPORTED_INPUT_FORMAT` | WARN | El formato de un Input Data (`params.format`) no es `csv`/`delta`/`yxdb` (ej. `excel`). | Se genera `read_csv_path` como fallback marcado con `# TODO`; para Excel usar `pandas.read_excel` + `spark.createDataFrame` (ver `tool_mapping.md`). |
| `WARN_UNSUPPORTED_OUTPUT_FORMAT` | WARN | El formato de un Output Data no es `delta`/`table`. | Se genera `write_delta` como fallback; confirmar el formato real deseado. |

## Clasificación de paridad (Fase 5)

No son códigos de log sino el veredicto final por workflow, calculado como
índice ponderado (ver `10_paridad_alteryx_pyspark.md` original / Fase 5 del
`SKILL.md`):

| Rango | Veredicto |
|---|---|
| ≥ 0.99 | `SUCCESS` |
| 0.95 – 0.99 | `REVIEW` |
| < 0.95 | `FAIL` |

## Regla para el modelo

Si durante una conversión el modelo detecta una situación que amerita una
advertencia y **no encuentra un código aquí que la describa exactamente**, no
debe inventar uno nuevo silenciosamente: debe señalarlo al usuario en texto
plano y sugerir que se añada un código nuevo a esta tabla (con su nivel y
acción) antes de generalizar el caso a otros workflows.
