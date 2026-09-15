# Lenguaje Formula de Alteryx → PySpark

El transpilador de expresiones (Fase 3) traduce lo que está en `params.expressions[].expr`
del modelo intermedio. Esta tabla cubre las funciones más frecuentes (ver el
`expressions.txt` que produce el inventario para priorizar por frecuencia real
en el portafolio antes de invertir tiempo en casos raros).

**Regla general:** no traduzcas una expresión Formula palabra por palabra sin
pasar antes por esta tabla. La semántica de NULL y de coerción de tipos difiere
lo suficiente entre Alteryx y Spark como para que una traducción literal
produzca resultados distintos en silencio.

## Condicionales

| Alteryx | PySpark | Nota |
|---|---|---|
| `IIF(cond, a, b)` | `F.when(cond, a).otherwise(b)` | Si `cond` es NULL, Alteryx devuelve NULL (no `b`). `F.when` con una condición NULL también da NULL por defecto en Spark — se comporta igual, pero verificalo si la condición involucra comparaciones que puedan dar NULL. |
| `IF cond THEN a ELSEIF cond2 THEN b ELSE c ENDIF` | `F.when(cond, a).when(cond2, b).otherwise(c)` | Encadenar `.when()`. |
| `ISNULL(x)` | `F.col(x).isNull()` | — |
| `IIF(ISNULL(x), y, x)` | `F.coalesce(F.col(x), F.lit(y))` | Patrón tan común que conviene generar siempre `coalesce` en vez del `when` literal. |

## Texto

| Alteryx | PySpark | Nota |
|---|---|---|
| `Trim(s)` / `TrimLeft(s)` / `TrimRight(s)` | `F.trim`, `F.ltrim`, `F.rtrim` | — |
| `Left(s, n)` / `Right(s, n)` | `F.substring(s, 1, n)` / `F.substring(s, -n, n)` | Alteryx es 1-indexado igual que `substring` de Spark. |
| `Length(s)` | `F.length(s)` | En Alteryx, `Length(NULL)` da error o 0 según versión; en Spark `F.length(NULL)` es NULL. Envolver en `F.coalesce(F.length(s), F.lit(0))` si el flujo original asumía 0. |
| `Uppercase(s)` / `Lowercase(s)` | `F.upper(s)` / `F.lower(s)` | — |
| `PadLeft(s, n, c)` / `PadRight(s, n, c)` | `F.lpad(s, n, c)` / `F.rpad(s, n, c)` | — |
| `Substring(s, start, len)` | `F.substring(s, start + 1, len)` | Alteryx es 0-indexado en `Substring`, Spark `F.substring` es 1-indexado: sumar 1 al `start`. Fuente común de off-by-one si se copia literal. |
| `Replace(s, buscar, reemplazo)` | `F.regexp_replace(s, re.escape(buscar), reemplazo)` | `Replace` de Alteryx es texto literal, no regex. Hay que escapar el patrón o usar una función de reemplazo literal si Spark la expone en la versión del runtime. |
| `REGEX_Replace(s, patron, reemplazo)` | `F.regexp_replace(s, patron, reemplazo)` | Este sí es regex en ambos lados — pero ver la nota de dialecto regex en `tool_mapping.md` (RegEx). |
| `REGEX_Match(s, patron)` | `F.col(s).rlike(patron)` | — |
| `REGEX_CountMatches(s, patron)` | `F.size(F.split(s, patron)) - 1` o `F.regexp_count` si el runtime de Fabric ya lo soporta (Spark 3.5+) | Verificar versión de Spark antes de asumir `regexp_count`. |
| `Contains(s, buscar)` | `F.col(s).contains(buscar)` | — |
| `StartsWith(s, buscar)` / `EndsWith(s, buscar)` | `F.col(s).startswith(buscar)` / `.endswith(buscar)` | — |
| `ToString(x)` | `F.col(x).cast("string")` | Formato de números puede diferir (decimales, notación científica). Si el resultado se usa para mostrar al usuario, comparar contra el formato exacto de Alteryx en la validación de paridad. |

## Números

| Alteryx | PySpark | Nota |
|---|---|---|
| `ToNumber(s)` | `F.col(s).cast("double")` (o `"int"` si se sabe que es entero) | `ToNumber` de una cadena no numérica da NULL en Alteryx y también en Spark (`cast` inválido → NULL), mismo comportamiento. |
| `Round(x, n)` | `F.round(x, n)` | — |
| `Ceiling(x)` / `Floor(x)` | `F.ceil(x)` / `F.floor(x)` | — |
| `Mod(a, b)` | `a % b` (operador de columna) o `F.pmod(a, b)` si se necesita resultado siempre no-negativo | El operador `%` en Spark puede devolver negativo con dividendo negativo, igual que Python; `Mod` de Alteryx también sigue el signo del dividendo — coinciden, pero si el flujo depende de un resultado no-negativo usar `F.pmod`. |
| `Abs(x)` | `F.abs(x)` | — |

## Fecha y hora

| Alteryx | PySpark | Nota |
|---|---|---|
| `DateTimeToday()` | `F.current_date()` | — |
| `DateTimeNow()` | `F.current_timestamp()` | — |
| `DateTimeAdd(fecha, n, "days"/"months"/"years")` | `F.date_add(fecha, n)` / `F.add_months(fecha, n)` / `F.add_months(fecha, n*12)` | No hay una sola función genérica de "sumar N unidades" en Spark; el generador debe ramificar según la unidad. |
| `DateTimeDiff(a, b, "days")` | `F.datediff(a, b)` | Para diferencias en meses/años usar `F.months_between` y dividir/redondear según la unidad pedida. |
| `DateTimeParse(s, formato)` | `F.to_date(s, formato_traducido)` / `F.to_timestamp(...)` | **El formato nunca se copia igual — corrección respecto a versiones previas de este documento.** Alteryx usa tokens estilo `strftime`/C (`%Y-%m-%d`, `%m/%d/%Y`, `%H:%M:%S`); Spark usa patrones `java.time` sin `%` (`yyyy-MM-dd`, `MM/dd/yyyy`, `HH:mm:ss`). Ningún token coincide letra por letra porque uno lleva `%` y el otro no — **siempre** hay que traducir, no solo en casos raros. Ver la tabla de conversión abajo; el transpilador (`scripts/formula_transpiler.py`) la aplica automáticamente. |
| `DateTimeFormat(fecha, formato)` | `F.date_format(fecha, formato_traducido)` | Misma traducción obligatoria que arriba. |

**Tabla de conversión `strftime` (Alteryx) → `java.time` (Spark):**

| Alteryx | Spark | Significado |
|---|---|---|
| `%Y` | `yyyy` | Año, 4 dígitos |
| `%y` | `yy` | Año, 2 dígitos |
| `%m` | `MM` | Mes, 2 dígitos |
| `%d` | `dd` | Día del mes, 2 dígitos |
| `%H` | `HH` | Hora 24h |
| `%I` | `hh` | Hora 12h |
| `%M` | `mm` | Minutos |
| `%S` | `ss` | Segundos |
| `%p` | `a` | AM/PM |
| `%A` | `EEEE` | Día de la semana, nombre completo |
| `%a` | `EEE` | Día de la semana, abreviado |
| `%B` | `MMMM` | Mes, nombre completo |
| `%b` | `MMM` | Mes, abreviado |
| `%j` | `DDD` | Día del año (1-366) |
| `%z` | `XX` | Offset de zona horaria |

Un token de formato que no esté en esta tabla no se traduce silenciosamente:
el transpilador lo deja tal cual y emite `WARN_UNSUPPORTED_DATE_TOKEN` para
revisión manual (ver `warnings.md`).

## Semántica de NULL — la trampa principal

- En Alteryx, una cadena vacía (`""`) y NULL **no son lo mismo**, pero varias
  herramientas (Formula incluida) tratan campos numéricos vacíos como NULL al
  leerlos. En Spark, `""` tampoco es NULL — la equivalencia suele mantenerse,
  pero conviene comprobar el dato de origen (CSV vacío vs. CSV con NULL literal)
  antes de asumirlo.
- Operadores de comparación (`=`, `<`, `>`) con un operando NULL devuelven NULL
  en ambos motores (no `False`) — coherente. Pero el **efecto en un `Filter`**
  no lo es automáticamente: Alteryx descarta la fila cuando la condición es
  NULL (se va a la salida no marcada), y `df.filter(cond)` en Spark hace lo
  mismo (una condición NULL no pasa el filtro). Coinciden, pero si alguien
  reescribe el filtro como `~cond` para tomar la rama contraria, **NULL tampoco
  pasa por `~cond`** en Spark (`NOT NULL` es NULL, no `True`) — mientras que en
  Alteryx la rama "False" de un Filter sí puede recibir esas filas según la
  herramienta. Si el flujo usa ambas salidas del Filter, generar explícitamente
  `df.filter(cond)` y `df.filter((~cond) | cond.isNull())` en vez de asumir que
  son complementarios.
- Aritmética con NULL: `NULL + 1` es NULL en ambos motores. Pero `SUM`/`AVG` en
  Spark ignoran NULL al agregar (ver `spark_patterns.md`, sección de paridad),
  mientras que una `Formula` fila a fila que combina un NULL en una suma da
  fila NULL, no la ignora. Esto es coherente con Alteryx a nivel de fila; el
  punto de fricción real aparece en las agregaciones posteriores, no en el
  Formula mismo.

## Qué no cubre esta tabla

Expresiones con funciones de Alteryx poco frecuentes, referencias a campos
`[Row-n:Campo]` (eso es Multi-Row Formula, ver `tool_mapping.md`), y macros
custom con lógica arbitraria. Si el inventario (`expressions.txt`) muestra una
función que no está aquí, añádela con su equivalencia antes de generalizar el
transpilador — no adivines la semántica de una función que no has verificado.
