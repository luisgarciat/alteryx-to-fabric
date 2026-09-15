---
name: alteryx-to-fabric
description: Migra flujos ETL de Alteryx (.yxmd/.yxzp) a notebooks PySpark para Microsoft Fabric, con trazabilidad por ToolID, QA automático y validación de paridad. Úsala siempre que se mencione Alteryx, .yxmd, .yxzp, .yxmc, migración de ETL a Fabric o Lakehouse, conversión de workflows a PySpark, o cuando alguien pregunte qué herramientas usa un flujo de Alteryx o cómo traducir una herramienta concreta (Join, Summarize, Multi-Row Formula, Cross Tab) a Spark. Aplica aunque no digan "migrar" explícitamente.
---

# Alteryx → Microsoft Fabric

Convierte workflows de Alteryx en notebooks PySpark ejecutables en Fabric.

**Principio rector:** lo determinista lo hace código, no el modelo. El parseo de XML
y la generación de código para herramientas ya mapeadas son scripts. El modelo
interviene donde hace falta juicio: expresiones Formula, inferencia de topología en
joins, herramientas no soportadas, macros custom y redacción del reporte.

Nunca escribas el IR JSON "a mano" leyendo el XML. Ejecuta el parser.

## Fases

La migración tiene cinco fases. Identifica en cuál está el usuario y entra ahí.

### Fase 1 — Inventario (antes que nada, si hay más de ~20 workflows)

Alteryx tiene cientos de herramientas; un portafolio real usa 25 o 30, y el 80% del
volumen está en 10. Construir mapeos sin este dato desperdicia semanas.

```bash
python scripts/inventory.py /ruta/a/workflows --out ./inventory --top 30
```

Produce `inventory.md` (legible), `inventory.json`, `tools.csv`, `workflows.csv`
y `expressions.txt`. Al presentarlo al usuario, enfócate en tres cosas:

1. **La curva de cobertura.** Cuántas herramientas hay que soportar para cubrir
   qué porcentaje de workflows. Esto define el alcance del sprint.
2. **Las macros custom.** No son traducibles automáticamente. Cada una necesita
   decisión humana: reimplementar, reemplazar por una función Spark, o descartar.
3. **El volumen de expresiones.** Dimensiona el trabajo del transpilador de Formula,
   que suele ser la mitad del esfuerzo total.

Si `inventory.md` reporta herramientas desconocidas (`__unknown__` o nombres raros),
añádelas a `PLUGIN_ALIASES` en `scripts/inventory.py` en vez de ignorarlas.

### Fase 2 — Parser a IR JSON

```bash
python scripts/parse_workflow.py flujo.yxmd                       # -> flujo.ir.json
python scripts/parse_workflow.py flujo.yxzp --out ./ir/flujo.json
python scripts/parse_workflow.py /ruta/a/workflows --out ./ir      # carpeta completa
```

Determinista: el mismo `.yxmd` produce siempre el mismo JSON (salvo
`metadata.extracted_at`). Reutiliza `PLUGIN_ALIASES` y el descubrimiento de
archivos de `scripts/inventory.py` — un alias nuevo ahí se hereda aquí solo.

Tiene extractor de `params` estructurado (no solo XML crudo) para las
herramientas de mayor frecuencia: Input Data, Output Data, Select, Filter,
Formula, Multi-Row Formula, Join, Union, Summarize. Cualquier otra herramienta
cae a `params._raw` (el XML de `<Configuration>` convertido a dict, sin
pérdida) — quien construya la Fase 4 debe interpretarlo caso por caso o
ampliar el extractor si el volumen lo justifica (revisar `inventory.md`).

Cosas que el parser ya resuelve por ti:

- **Contenedores (`ToolContainer`) se aplanan.** Sus herramientas internas
  quedan como nodos de primer nivel; el contenedor mismo no aparece en el IR
  (es cosmético — igual que `Comment`/`TextBox`/`Browse`).
- **`WARN_ORDER_UNDEFINED` se emite automáticamente**: si un Multi-Row
  Formula, Running Total o Record ID no tiene un `Sort` explícito
  inmediatamente aguas arriba, el nodo queda marcado en
  `metadata.warnings`. En `strict`, trátalo como bloqueante — pide la columna
  de orden al usuario antes de generar código para ese nodo.
- **El `how` de un Join NO se resuelve aquí a propósito.** El parser solo
  guarda `join_keys` (izquierda/derecha) y las conexiones con su ancla real
  (`Left`/`Right`/`Join` — no las abreviaturas `J`/`L`/`R` de la documentación
  heredada del GPT anterior). Inferir `inner`/`left`/`right`/`outer` es
  trabajo de la Fase 3/4, ya sea con una heurística sobre esas anclas o con
  juicio del modelo cuando sea ambiguo.

Fixtures sintéticos para probar cambios al parser en `fixtures/*.yxmd` — no
son casos reales, son XML de Alteryx escrito a mano para ejercitar: cosméticos
+ contenedores (`01_simple`), un Join con Summarize (`02_join_summarize`), y
`WARN_ORDER_UNDEFINED` + detección de macro (`03_formula_order_macro`). Antes
de tocar el parser, correlo contra los tres y compara con `inventory.py` sobre
la misma carpeta — deben coincidir en herramientas y conteos.

### Fase 3 — Transpilador de expresiones Formula

```bash
python scripts/transpile_formula.py flujo.ir.json                  # -> flujo.transpiled.json
python scripts/transpile_formula.py --expr 'IIF([monto]>100,"alto","bajo")'   # prueba rapida, sin IR
```

Es un compilador de verdad (tokenizer → parser recursivo → AST → codegen),
no reemplazos de texto con regex — ver `scripts/formula_transpiler.py`. Toma
el IR de la Fase 2 y le agrega `pyspark_code` a cada expresión de los nodos
`Formula`, `Multi-Field Formula`, `Filter` y `Multi-Row Formula`.

Para Multi-Row Formula, las referencias `[Row-n:Campo]` se traducen a
`F.lag`/`F.lead` sobre una variable `Window` que se nombra por convención
`w_<ToolID>` — este script **no la define**, solo genera código que la asume.
Definir esa `Window` (partición por `group_by`, orden por la columna que se
confirme con el usuario tras el `WARN_ORDER_UNDEFINED` de la Fase 2) es
trabajo de la Fase 4.

Cuando el modelo necesite traducir una expresión suelta que no pasó por este
script (una excepción puntual, un caso de exploración), usa igual el
transpilador (`--expr`) en vez de traducirla "a ojo": la gramática tiene
casos no obvios (precedencia, `[Row-1:Campo]`, NULL) que un LLM comete errores
sutiles al traducir de memoria.

Ver `references/formula_language.md` para la tabla de equivalencias en la que
se basa el transpilador, y la sección "Transpilador de expresiones Formula"
en `references/warnings.md` para los códigos que puede emitir
(`WARN_UNSUPPORTED_FUNCTION`, `WARN_AMBIGUOUS_PLUS`,
`WARN_ROW_REF_WITHOUT_WINDOW`, etc.) — en `strict`, un
`WARN_PARSE_ERROR` o `WARN_ROW_REF_WITHOUT_WINDOW` debe bloquear el nodo
hasta revisión humana; el resto son señales para el reporte, no bloqueos.

### Fase 4 — Generación de notebook

```bash
python scripts/generate_notebook.py flujo.transpiled.json                                   # -> flujo.ipynb, modo lenient
python scripts/generate_notebook.py flujo.transpiled.json --modo strict --lakehouse-name VentasLH
```

Anda el grafo completo del IR en orden topológico (no asume un único
`df_in`→`df_out` lineal: resuelve joins, dos salidas de Filter, y varios
Input/Output Data) y genera un `.ipynb` con las secciones SETUP / READ /
TRANSFORMS / QA / OUTPUTS / LOGGING. Cada bloque conserva
`# TOOL: <nombre>` y `# SOURCE: ToolID=<id>`.

Resuelve automáticamente, sin intervención del modelo:

- **El `how` de cada Join**, a partir de qué anclas (`Left`/`Right`/`Join`)
  están conectadas aguas abajo — y el caso de llaves con nombre distinto
  por lado (renombra antes del join en vez de fallar).
- **La `Window` de cada Multi-Row Formula**: si tiene un `Sort` explícito
  inmediatamente aguas arriba, usa esa(s) columna(s) como `orderBy` real; si
  no, cae a `monotonically_increasing_id()` marcado con `WARN_ORDER_UNDEFINED`
  (en `lenient`) o bloquea la generación (en `strict`) — cierra el ciclo
  que la Fase 2 dejó abierto.
- **Unions que reconstruyen ramas de un Join** (mismo Join alimentando el
  mismo Union por dos anclas distintas): detecta la duplicación, no genera
  la unión redundante, y avisa con `WARN_UNION_AMBIGUO_POST_JOIN` para que
  se confirme el `how` inferido.

En `strict`, cualquier condición bloqueante (herramienta sin generador,
Macro, Join ambiguo, orden no confirmado, expresión sin transpilar) hace que
el script **no escriba el notebook** — imprime la lista y sale con error. En
`lenient`, genera igual con `# TODO` ligado al ToolID y lo cuenta en el
payload final de LOGGING.

Herramientas con generador propio hoy: Input Data, Output Data, Select,
Filter, Formula, Multi-Row Formula, Join, Union, Summarize, Sort. Todo lo
demás (incluyendo `Multi-Field Formula`, que todavía no tiene extractor en
la Fase 2) cae a un passthrough marcado con `ERR_UNSUPPORTED_TOOL` — amplía
`TRANSFORM_HANDLERS` en `scripts/generate_notebook.py` según lo que muestre
`inventory.md` como frecuente antes de invertir tiempo en herramientas raras.

### Fase 5 — QA y paridad

```bash
pytest scripts/test_parity_example.py -v   # ejemplo runnable + prueba de regresion de parity.py
```

Compara la salida real de Alteryx (exportada a CSV/Parquet para un dataset de
prueba) contra la salida del notebook generado en la Fase 4, con 5 métricas
ponderadas (`row_count`, `aggregates`, `schema_types`, `null_integrity`,
`value_domains`) — índice ≥0.99 `SUCCESS`, 0.95–0.99 `REVIEW` (no bloquea,
requiere ojo humano), <0.95 `FAIL` (el test lanza `AssertionError` con el
detalle completo).

Para cada workflow migrado, copia el patrón de
`scripts/test_parity_example.py` en un archivo propio (`test_<workflow>_parity.py`):
carga la referencia y el candidato con `parity.load_csv_as_records()` (para
pilotos chicos, sin Spark) o pasa DataFrames reales a
`parity.assert_parity_spark()` (en Fabric, agrega en el motor — nunca hace
`.collect()` de las filas completas), configura las reglas de agregación
propias de ese workflow, y deja que `pytest` lo enganche a CI en vez de
depender de que alguien se acuerde de revisarlo a mano.

Ver `references/parity.md` para la definición completa de cada métrica, las
tolerancias, y por qué no hay que mezclar estadísticas calculadas con Spark
y calculadas en Python puro en la misma comparación.

## Modos

- `strict`: una herramienta no soportada o una macro custom detiene la conversión.
- `lenient`: continúa, inserta `# TODO` ligado al ToolID y lo registra.

Por defecto `strict` para workflows de producción financiera o regulatoria.

## Advertencias

Los códigos `WARN_*` los emite el generador, no el modelo. Ver `references/warnings.md`.
La más importante es `WARN_ORDER_UNDEFINED`: Alteryx preserva orden de registro y
Spark no. Si un Multi-Row Formula, Record ID o Sample no tiene clave de orden
explícita, el resultado no es determinista y eso bloquea la conversión en `strict`.

## Referencias

- `references/tool_mapping.md` — una entrada por herramienta Alteryx.
- `references/formula_language.md` — expresiones y diferencias de semántica.
- `references/spark_patterns.md` — snippets de joins seguros, ventanas, Delta.
- `references/warnings.md` — catálogo de códigos WARN_*.
- `references/parity.md` — métricas de paridad, tolerancias, y cómo escribir el test de un workflow.
