# alteryx-to-fabric

Paquete de migración Alteryx → PySpark/Microsoft Fabric. Las 5 fases están
listas: inventario, parser a IR JSON, transpilador Formula, generación de
notebook, y validación de paridad.

## Cómo correrlo

Sin dependencias: solo Python 3.9+.

```bash
python scripts/inventory.py /ruta/a/tus/workflows --out ./inventory --top 30
```

Acepta `.yxmd`, `.yxmc` y `.yxzp` (abre el zip y lee el `.yxmd` de adentro),
recursivamente. No modifica nada: solo lee.

### Salidas

| Archivo | Qué contiene |
|---|---|
| `inventory.md` | Reporte legible. Empieza por aquí. |
| `inventory.json` | Lo mismo, estructurado, para automatizar. |
| `tools.csv` | Histograma completo de herramientas. |
| `workflows.csv` | Un renglón por workflow, ordenado por complejidad. |
| `expressions.txt` | Expresiones Formula encontradas (muestra por workflow). |

### Cómo leer el reporte

**Curva de cobertura.** La columna "Workflows completos" dice cuántos workflows
quedarían 100% convertibles si soportamos las primeras K herramientas. Busca el
punto donde la curva se aplana: ahí está el límite del sprint 1.

**Macros custom.** Cada una necesita decisión humana. Si el número es alto, la
migración cambia de forma y conviene tratarlas como un proyecto aparte.

**Piloto sugerido.** Complejidad mediana y sin macros. Migrar ese workflow a mano
primero da la salida de referencia contra la cual medir paridad después.

## Limitaciones conocidas

El parser se apoya en la estructura estándar de `AlteryxDocument`
(`Nodes` / `Node` / `GuiSettings@Plugin` / `Connections`), y en el convenio de
nombres `Assembly.Tool.Tool`. Funciona con las versiones recientes de Alteryx
Designer, pero **hay que validarlo contra archivos reales**.

Si el reporte muestra herramientas con nombre `__unknown__`, nombres internos que
no reconoces, o un conteo que no cuadra con lo que ves en el canvas, la tabla
`PLUGIN_ALIASES` en `scripts/inventory.py` necesita ampliarse. Es una sola tabla y
el arreglo es de una línea por herramienta.

Las herramientas cosméticas (Comment, TextBox, Browse, contenedores) se cuentan
pero se excluyen de las métricas de complejidad y cobertura.

## Fase 2 — Parser a IR JSON

```bash
python scripts/parse_workflow.py flujo.yxmd                  # -> flujo.ir.json
python scripts/parse_workflow.py /ruta/a/workflows --out ./ir
```

Convierte `.yxmd`/`.yxmc`/`.yxzp` al modelo intermedio JSON (`workflow`,
`nodes`, `connections`, `metadata`). Determinista y sin dependencias externas.
Aplana contenedores, excluye herramientas cosméticas, y marca
`WARN_ORDER_UNDEFINED` automáticamente cuando un Multi-Row Formula / Running
Total / Record ID no tiene un Sort explícito aguas arriba. El `how` de los
Join se deja sin resolver a propósito — eso es trabajo de la fase siguiente.

Antes de modificar el parser, corrélo contra `fixtures/*.yxmd` y compara con
`inventory.py` sobre la misma carpeta (deben coincidir en herramientas y
conteos):

```bash
python scripts/parse_workflow.py fixtures --out /tmp/ir_check
python scripts/inventory.py fixtures --out /tmp/inv_check
```

## Fase 3 — Transpilador de expresiones Formula

```bash
python scripts/transpile_formula.py flujo.ir.json             # -> flujo.transpiled.json
python scripts/transpile_formula.py --expr 'Left([nombre], 3)' # prueba rapida sin IR
```

Tokenizer + parser recursivo + AST + codegen para el lenguaje Formula de
Alteryx (no regex). Agrega `pyspark_code` a cada expresión de nodos `Formula`,
`Multi-Field Formula`, `Filter` y `Multi-Row Formula` en el IR. Las
referencias `[Row-n:Campo]` de Multi-Row Formula generan `F.lag`/`F.lead`
sobre una `Window` nombrada `w_<ToolID>` que la Fase 4 debe definir.

```bash
python scripts/parse_workflow.py fixtures/03_formula_order_macro.yxmd --out /tmp/f.json
python scripts/transpile_formula.py /tmp/f.json
```

## Fase 4 — Generación de notebook

```bash
python scripts/generate_notebook.py flujo.transpiled.json --modo lenient --lakehouse-name VentasLH
```

Toma el IR ya transpilado y genera un `.ipynb` completo (SETUP/READ/
TRANSFORMS/QA/OUTPUTS/LOGGING), andando el grafo en orden topológico. Resuelve
el `how` de los Join según qué anclas quedaron conectadas, arma la `Window`
de cada Multi-Row Formula a partir de un `Sort` explícito aguas arriba (o cae
a un orden no determinista marcado como warning), y detecta Uniones que
reconstruyen ramas de un Join para no duplicar filas. En `strict` no escribe
el notebook si queda algo bloqueante sin resolver (herramienta sin soporte,
Macro, Join ambiguo, orden sin confirmar).

```bash
python scripts/parse_workflow.py fixtures/03_formula_order_macro.yxmd --out /tmp/f.json
python scripts/transpile_formula.py /tmp/f.json
python scripts/generate_notebook.py /tmp/f.transpiled.json --modo lenient
```

## Fase 5 — QA y validación de paridad

```bash
pytest scripts/test_parity_example.py -v
```

`scripts/parity.py` compara la salida real de Alteryx contra la del notebook
generado con 5 métricas ponderadas (`row_count`, `aggregates`,
`schema_types`, `null_integrity`, `value_domains`) y un índice de 0 a 1:
≥0.99 `SUCCESS`, 0.95–0.99 `REVIEW` (no bloquea), <0.95 `FAIL` (lanza
`AssertionError`, se engancha directo a CI). Funciona sin Spark para pilotos
chicos (`compute_stats_from_records` + `load_csv_as_records`) o con Spark
real en Fabric (`compute_stats_spark`, agrega en el motor). Ver
`references/parity.md` para la definición de cada métrica y cómo escribir el
test de un workflow real — copia el patrón de `scripts/test_parity_example.py`.

## Estructura

```
alteryx-to-fabric/
├── SKILL.md                    # el router de la skill (las 5 fases)
├── scripts/
│   ├── inventory.py            # fase 1
│   ├── parse_workflow.py       # fase 2
│   ├── formula_transpiler.py   # fase 3 — libreria (tokenizer/parser/codegen)
│   ├── transpile_formula.py    # fase 3 — CLI sobre el IR
│   ├── generate_notebook.py    # fase 4 — IR transpilado -> .ipynb
│   ├── parity.py               # fase 5 — libreria de metricas de paridad
│   └── test_parity_example.py  # fase 5 — ejemplo runnable (pytest)
├── references/                 # tool_mapping, formula_language, spark_patterns, warnings, parity
└── fixtures/                   # workflows sinteticos + fixtures/parity/ (CSVs calibrados por veredicto)
```
