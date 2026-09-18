# Mapeo de herramientas Alteryx → PySpark

Una entrada por herramienta. `Snippet` referencia la función reutilizable en
`spark_patterns.md`; si no existe todavía, el código va inline.

Nombres normalizados según `PLUGIN_ALIASES` en `scripts/inventory.py` — si el
inventario reporta una herramienta que no está aquí, añádela primero a esa
tabla (para que se cuente bien) y luego a este archivo.

## Entrada/Salida

| Herramienta Alteryx | PySpark / Fabric | Snippet | Notas |
|---|---|---|---|
| Input Data | `spark.read.table(...)` / `.format('delta').load(...)` / `.csv(...)` | `read_table`, `read_delta_path`, `read_csv_path` | El formato origen (DB, CSV, Excel) determina la función. Excel no tiene lector nativo de Spark: usar `pandas.read_excel` + `spark.createDataFrame` para volúmenes chicos, o convertir a CSV/Parquet antes de migrar. |
| Output Data | `df.write.format('delta').save(...)` / `.saveAsTable(...)` | `write_delta`, `save_as_table` | Preservar el modo (`overwrite`/`append`) que tenía el Output original; Alteryx por defecto sobrescribe. |
| Dynamic Input | Lectura parametrizada por wildcard/patrón | — | Traducir el patrón de archivos a un `glob` sobre la carpeta del Lakehouse, o a una lista de rutas resuelta antes de la lectura. Requiere revisión manual del patrón. |
| Text Input | `spark.createDataFrame([...], schema=...)` | — | Datos embebidos en el propio workflow; copiarlos tal cual. |

## Selección y limpieza

| Herramienta Alteryx | PySpark / Fabric | Snippet | Notas |
|---|---|---|---|
| Select | `df.select(...)`, `.withColumnRenamed(...)`, `.drop(...)` | — | Select también fija el orden final de columnas y puede cambiar tipos (`SelectFields/Type`); si cambia tipo, emitir un `.cast(...)` explícito, no confiar en inferencia. |
| Dynamic Select | Selección de columnas por expresión regular sobre `df.columns` | — | Traducir el patrón (`*_id`, tipo de dato, etc.) a un filtro Python sobre `df.schema` antes del `.select`. |
| Dynamic Rename | `df.toDF(*nuevos_nombres)` o `reduce(lambda d, kv: d.withColumnRenamed(*kv), pares, df)` | — | Si renombra por posición (modo "Take Field Names From First Row") requiere leer una fila de metadatos primero; no es un mapeo 1:1 directo. |
| Data Cleansing | `.trim()`, `F.regexp_replace` para caracteres especiales, `.na.fill("")` | — | Revisar exactamente qué casillas están activadas (nulls a vacío, trim, mayúsculas) — no aplicar todas por default. |
| Filter | `df.filter(cond)` para la salida `True`; `df.filter(~cond)` o `df.filter(F.expr(...) == False)` para `False` | — | Filter tiene dos salidas (`T`/`F`). Si solo se usa una rama, generar solo esa; si se usan ambas, generar dos DataFrames a partir del mismo predicado. |
| Unique | `df.dropDuplicates(subset=[...])` | — | Alteryx `Unique` también expone una salida "duplicados" opcional; si el workflow la usa, calcular el complemento (`df.exceptAll(df_unique)`). |
| Sample / Random % Sample | `df.limit(n)` (primeros N) / `df.sample(fraction, seed=...)` (aleatorio) | — | Fijar `seed` explícito para que la conversión sea reproducible en QA de paridad. |
| Sort | `df.orderBy(...)` | `auto_cast_for_sort` | Ver `WARN_ORDER_UNDEFINED` en `warnings.md`: Spark no garantiza orden de salida a menos que el orden se preserve explícitamente hasta el `write`. |

## Combinación de datos

| Herramienta Alteryx | PySpark / Fabric | Snippet | Notas |
|---|---|---|---|
| Join | `df_left.join(df_right, on=keys, how=...)` | `join_df`, `safe_join`, `rename_right`, `broadcast_join` | El `how` **no** viene explícito en Alteryx: hay que inferirlo de qué salidas están conectadas. El ancla real en el XML (`Connection=` en `<Origin>`/`<Destination>`) es literalmente `Left`, `Right` o `Join` — no las abreviaturas `J`/`L`/`R` que usa la documentación heredada del paquete GPT (confirmado parseando fixtures reales; ver `08_politicas_appendix_join_inferencia.md` en `Contexto Previo GPT/` para la versión original con esa notación). Usar `safe_join` siempre que ambos lados puedan tener columnas con el mismo nombre fuera de las keys. |
| Join Multiple | Encadenar `join_df`/`safe_join` en secuencia, o `functools.reduce` sobre la lista de DataFrames | — | Revisar si el modo es "join en cascada" (cada uno contra el resultado anterior) o "todos contra el primero"; Alteryx lo decide por configuración, no es siempre lo mismo. |
| Union | `df_a.unionByName(df_b, allowMissingColumns=True)` | — | Nunca usar `union()` a secas (posicional): los esquemas de Alteryx casi nunca llegan en el mismo orden de columnas. |
| Append Fields | `df_left.crossJoin(df_right)` | — | Solo es seguro si `df_right` es de una sola fila (caso típico: parámetros/totales). Si no, es un error de diseño heredado del flujo original — señalarlo, no traducirlo ciegamente. |

## Agregación y reestructuración

| Herramienta Alteryx | PySpark / Fabric | Snippet | Notas |
|---|---|---|---|
| Summarize | `df.groupBy(keys).agg(...)` | `group_agg` | Alteryx permite "Group By" implícito (sin key) para un resumen global: en ese caso omitir `groupBy` y usar `.agg(...)` directo sobre el DataFrame completo. |
| Cross Tab | `df.groupBy(keys).pivot(col).agg(...)` | `pivot_agg` | Si Alteryx tenía "Method for Aggregating Values" = concatenar strings, usar `F.collect_list` + `F.concat_ws` en vez de una función numérica. |
| Transpose | `df.select(keys + [F.expr(f"stack({n}, ...)").alias(...)])` (unpivot) | — | Es el inverso de Cross Tab. El número de columnas a transponer debe conocerse en tiempo de generación (no es dinámico en Spark sin un `stack` construido a mano). |
| Running Total | `F.sum(col).over(Window.partitionBy(keys).orderBy(orden_col).rowsBetween(Window.unboundedPreceding, 0))` | — | Requiere una columna de orden explícita — si el workflow no la tiene, es `WARN_ORDER_UNDEFINED`. |
| Record ID | `F.row_number().over(Window.orderBy(orden_col))` si debe ser secuencial y estable; `F.monotonically_increasing_id()` solo si el requisito es "un ID único", no un orden específico | — | Mismo problema de orden que Running Total. No usar `monotonically_increasing_id` cuando el flujo original esperaba una secuencia 1..N contigua: esa función no lo garantiza. |
| Multi-Row Formula | `F.lag(col, n).over(Window.partitionBy(keys).orderBy(orden_col))` / `F.lead(...)` | — | `[Row-1:Campo]` → `lag`, `[Row+1:Campo]` → `lead`. Si la fórmula actualiza el propio campo de forma acumulativa (referencia circular a la fila anterior ya calculada), no hay traducción directa por columnas: se necesita una UDF con estado o reescribir la lógica como ventana acumulada (`Window...rowsBetween`) según el caso concreto. Señalar para revisión manual si no es un lag/lead simple. |

## Transformación de texto y fecha

| Herramienta Alteryx | PySpark / Fabric | Snippet | Notas |
|---|---|---|---|
| Formula | Expresión traducida por el transpilador (Fase 3) | — | Ver `formula_language.md` para equivalencias función por función. |
| Multi-Field Formula | Igual que Formula pero aplicada en bucle sobre una lista de columnas (`for c in campos: df = df.withColumn(c, expr)`) | — | Confirmar si "Change Output Field Name" está activo (crea columnas nuevas) o no (sobrescribe in place). |
| Multi-Field Binary | Combina N columnas en 1 (ej. concatenar flags) | — | Mapear a `F.concat`/`F.array`/expresión aritmética según el operador binario configurado; no hay snippet genérico porque el operador varía. |
| RegEx (Parse/Match/Replace/Tokenize) | `regex_extract_col`, `regex_replace_col`, `regex_count_matches` | `regex_extract_col`, `regex_replace_col`, `regex_count_matches` | La sintaxis regex de Alteryx (.NET) y la de Spark (Java) difieren en detalles: named groups, lookbehind de longitud variable, `\d`/`\w` con Unicode. Revisar cada patrón, no asumir portabilidad 1:1. |
| Text To Columns | `F.split(col, sep)` + `.getItem(i)` por cada columna resultante, o `F.explode` si la config es "a filas" en vez de "a columnas" | — | Alteryx permite un número dinámico de columnas de salida; en Spark el número de columnas debe fijarse en tiempo de generación. |
| Find Replace | Join contra tabla de lookup + `F.coalesce(valor_reemplazo, valor_original)` | — | Es un join disfrazado (busca en una lista/tabla), no una función de texto simple. |
| DateTime | `F.to_date`/`F.to_timestamp`/`F.date_format` según la conversión configurada | `to_date_col`, `to_timestamp_col` | Ver `formula_language.md` para el mapeo de patrones de formato de fecha (Alteryx usa tokens tipo `.NET`, Spark usa patrones `java.time`). |
| Generate Rows | Bucle expandido antes de la carga (generación de secuencia) → `F.sequence(start, stop, step)` + `F.explode` | — | Traducible solo cuando la condición de parada es una expresión simple sobre un contador; si depende de estado acumulado fila a fila, requiere UDF o rediseño. |

## No traducibles automáticamente

| Herramienta Alteryx | Tratamiento |
|---|---|
| Macro (`MACRO::<nombre>`) | Nunca se traduce sola. Requiere decisión humana: reimplementar su lógica interna como función Spark, sustituir por un paso equivalente ya existente, o descartarla. Ver el inventario (`macros` en `inventory.json`) para priorizar cuáles vale la pena reimplementar. |
| Herramientas con `__unknown__` | El parser no reconoció el plugin. Añadir el alias correcto en `PLUGIN_ALIASES` (`scripts/inventory.py`) antes de intentar mapearla. |
| Block Until Done | Es control de ejecución de Alteryx (orden secuencial forzado), no una transformación de datos. En un notebook, el orden de las celdas ya cumple ese rol — se omite, documentando por qué en el reporte. |

## Cosméticas (se ignoran)

`Comment`, `TextBox`, `ExplorerBox`, `Browse`, `BrowseV2` (el reemplazo moderno
de `Browse` — visto en workflows reales de Alteryx 2023.1, agregado tras
probar la skill contra un flujo de producción), contenedores (`ToolContainer`).
No generan código; el inventario ya las excluye de las métricas de complejidad.

## Nota de compatibilidad — forma del modelo intermedio

`01_modelo_intermedio_schema.json` representa conexiones como
`{source_id, source_anchor, destination_id, destination_anchor}` y `outputs`/`inputs`
de cada nodo como listas planas de IDs. El helper `join_inference.py` (heredado
del paquete GPT) espera en cambio `outputs` como lista de objetos `{"name", "to"}`
y conexiones como `{"from": {"tool_id","output"}, "to": {"tool_id","input"}}`.

**Son formas distintas.** Quien construya el parser de la Fase 2 debe elegir una
sola (se recomienda la de `01_modelo_intermedio_schema.json`, por ser la que
tiene JSON Schema formal) y adaptar `join_inference.py` a esa forma antes de
usarlo — no asumir que conecta tal cual.
