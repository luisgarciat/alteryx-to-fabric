# Snippets PySpark reutilizables (Fabric Runtime 1.3+)

Bloques de código para que el generador de notebooks (Fase 4) los inyecte tal
cual, o casi tal cual, al traducir una herramienta ya mapeada en
`tool_mapping.md`. Convención: `F` = `pyspark.sql.functions`,
`W`/`Window` = `pyspark.sql.window.Window`, `df` = `DataFrame`.

Origen: portado y ampliado de `04_snippets_pyspark_reutilizables.md` (paquete
GPT v0.2.0), con los patrones de ventana que faltaban para Running Total,
Record ID y Multi-Row Formula.

## Importaciones estándar

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
```

## Lectura y escritura

```python
def read_table(table_fqn: str) -> DataFrame:
    return spark.read.table(table_fqn)

def read_delta_path(path: str) -> DataFrame:
    return spark.read.format('delta').load(path)

def read_csv_path(path: str, header: bool = True, inferSchema: bool = True) -> DataFrame:
    return spark.read.option('header', header).option('inferSchema', inferSchema).csv(path)

def write_delta(df: DataFrame, path: str, mode: str = 'overwrite', partitionBy: list[str] | None = None):
    w = df.write.format('delta').mode(mode)
    if partitionBy:
        w = w.partitionBy(*partitionBy)
    w.save(path)

def save_as_table(df: DataFrame, table_fqn: str, mode: str = 'overwrite', partitionBy: list[str] | None = None):
    w = df.write.mode(mode)
    if partitionBy:
        w = w.partitionBy(*partitionBy)
    w.saveAsTable(table_fqn)
```

`inferSchema=True` en CSV es cómodo para prototipar pero no determinista entre
corridas si cambia el muestreo; para producción, preferir un esquema explícito
una vez que el piloto valida los tipos.

## Joins

```python
def join_df(df_left: DataFrame, df_right: DataFrame, keys: list[str], how: str = 'inner') -> DataFrame:
    return df_left.join(df_right, on=keys, how=how)

def rename_right(df_right: DataFrame, keys: list[str], prefix: str = 'r_') -> DataFrame:
    rename_exprs = []
    for c in df_right.columns:
        if c in keys:
            rename_exprs.append(F.col(c))
        else:
            rename_exprs.append(F.col(c).alias(f'{prefix}{c}'))
    return df_right.select(*rename_exprs)

def safe_join(df_left: DataFrame, df_right: DataFrame, keys: list[str], how: str = 'inner', right_prefix: str = 'r_') -> DataFrame:
    df_right_renamed = rename_right(df_right, keys, prefix=right_prefix)
    return df_left.join(df_right_renamed, on=keys, how=how)

def broadcast_join(df_left: DataFrame, df_small: DataFrame, keys: list[str], how: str = 'inner') -> DataFrame:
    return df_left.join(F.broadcast(df_small), on=keys, how=how)
```

Usar `safe_join` por default salvo que se sepa con certeza que no hay columnas
duplicadas fuera de las keys — un `join` normal con columnas homónimas produce
un DataFrame ambiguo que falla (o peor, toma la columna equivocada) en pasos
posteriores. `broadcast_join` solo cuando `df_small` cabe cómodo en memoria de
cada executor (equivalente al Join de Alteryx en modo "in-memory" para el lado
chico).

**El `how` no viene explícito en el XML de Alteryx** — se infiere de las
salidas conectadas (`J`/`L`/`R`). Ver la nota de inferencia y el problema de
compatibilidad de esquema en `tool_mapping.md` → sección Join antes de generar
este bloque automáticamente.

## Patrones de ventana (Running Total, Record ID, Multi-Row Formula)

Todos comparten el mismo requisito: una columna de orden explícita. Si no
existe en el modelo intermedio, es `WARN_ORDER_UNDEFINED` (ver `warnings.md`)
y en modo `strict` debe bloquear la generación, no adivinar un orden.

```python
def running_total(df: DataFrame, partition_cols: list[str], order_col: str, value_col: str, out_col: str = 'running_total') -> DataFrame:
    w = Window.partitionBy(*partition_cols).orderBy(order_col).rowsBetween(Window.unboundedPreceding, 0)
    return df.withColumn(out_col, F.sum(value_col).over(w))

def record_id(df: DataFrame, order_col: str, out_col: str = 'RecordID') -> DataFrame:
    '''Secuencia 1..N estable — no usar monotonically_increasing_id() si el
    flujo original espera contigüidad.'''
    w = Window.orderBy(order_col)
    return df.withColumn(out_col, F.row_number().over(w))

def lag_lead_columns(df: DataFrame, partition_cols: list[str], order_col: str, value_col: str, offsets: list[int]) -> DataFrame:
    '''offsets negativos = Row-n (lag), positivos = Row+n (lead).'''
    w = Window.partitionBy(*partition_cols).orderBy(order_col)
    for n in offsets:
        if n < 0:
            df = df.withColumn(f'{value_col}_row{n}', F.lag(value_col, -n).over(w))
        elif n > 0:
            df = df.withColumn(f'{value_col}_row+{n}', F.lead(value_col, n).over(w))
    return df
```

`record_id` con `row_number()` fuerza a Spark a mover todos los datos a una
sola partición lógica de orden — costoso en volúmenes grandes. Si el único
requisito real es "un identificador único", no una secuencia 1..N, usar
`F.monotonically_increasing_id()` en su lugar y documentarlo como decisión
consciente, no como default silencioso.

## Transformaciones y fórmulas

```python
def to_date_col(df: DataFrame, src: str, fmt: str, dst: str = 'dt') -> DataFrame:
    return df.withColumn(dst, F.to_date(F.col(src), fmt))

def to_timestamp_col(df: DataFrame, src: str, fmt: str, dst: str = 'ts') -> DataFrame:
    return df.withColumn(dst, F.to_timestamp(F.col(src), fmt))

def fill_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    return df.na.fill(fill_map)

def drop_nulls(df: DataFrame, subset: list[str] | None = None) -> DataFrame:
    return df.na.drop(subset=subset)

def auto_cast_for_sort(df: DataFrame, cols: list[str]) -> DataFrame:
    '''Detecta columnas usadas en ordenaciones y aplica cast a numérico si
    contienen solo dígitos.'''
    import re
    for c in cols:
        try:
            sample = df.select(c).limit(100).toPandas()[c].astype(str)
            if all(re.match(r"^[0-9]+$", x) for x in sample if x not in ("None", "nan")):
                df = df.withColumn(c, F.col(c).cast("int"))
                print(f"[INFO_CAST_APPLIED] Columna {c} convertida a int para ordenación.")
        except Exception as e:
            print(f"[WARN_CAST_SKIP] No se pudo evaluar columna {c}: {str(e)}")
    return df
```

## Regex

```python
def regex_extract_col(df: DataFrame, src: str, pattern: str, group_idx: int, dst: str) -> DataFrame:
    return df.withColumn(dst, F.regexp_extract(F.col(src), pattern, group_idx))

def regex_replace_col(df: DataFrame, src: str, pattern: str, replacement: str, dst: str | None = None) -> DataFrame:
    col = F.regexp_replace(F.col(src), pattern, replacement)
    return df.withColumn(dst if dst else src, col)

def regex_count_matches(df: DataFrame, src: str, pattern: str, dst: str = 'match_count') -> DataFrame:
    return df.withColumn(dst, F.size(F.split(F.col(src), pattern)) - 1)
```

Ver `formula_language.md` para las diferencias de dialecto regex (.NET vs Java)
antes de asumir que un patrón de Alteryx es portable tal cual.

## Agregaciones

```python
def group_agg(df: DataFrame, keys: list[str], aggs: dict[str, tuple[str, str]]) -> DataFrame:
    agg_exprs = []
    for dst, (fn, field) in aggs.items():
        fn_l = fn.lower()
        if fn_l == 'sum':
            agg_exprs.append(F.sum(field).alias(dst))
        elif fn_l in ('avg', 'mean'):
            agg_exprs.append(F.avg(field).alias(dst))
        elif fn_l == 'min':
            agg_exprs.append(F.min(field).alias(dst))
        elif fn_l == 'max':
            agg_exprs.append(F.max(field).alias(dst))
        elif fn_l == 'count':
            agg_exprs.append(F.count(field).alias(dst))
        elif fn_l in ('countdistinct', 'count_distinct'):
            agg_exprs.append(F.countDistinct(field).alias(dst))
        else:
            raise ValueError(f'Función no soportada: {fn}')
    return df.groupBy(*keys).agg(*agg_exprs)
```

## Pivot / Cross Tab

```python
def pivot_agg(df: DataFrame, keys: list[str], pivot_col: str, agg_fn: str, agg_field: str, fill_none: int | float | str | None = 0):
    fn = agg_fn.lower()
    if fn == 'sum':
        res = df.groupBy(*keys).pivot(pivot_col).agg(F.sum(agg_field))
    elif fn in ('avg', 'mean'):
        res = df.groupBy(*keys).pivot(pivot_col).agg(F.avg(agg_field))
    elif fn == 'min':
        res = df.groupBy(*keys).pivot(pivot_col).agg(F.min(agg_field))
    elif fn == 'max':
        res = df.groupBy(*keys).pivot(pivot_col).agg(F.max(agg_field))
    elif fn == 'count':
        res = df.groupBy(*keys).pivot(pivot_col).agg(F.count(agg_field))
    else:
        raise ValueError(f'Función de agregación no soportada: {agg_fn}')
    return res.fillna(fill_none)
```

## Paridad numérica tras joins no-inner

Después de un `left`/`right`/`outer`, el lado sin match trae NULL en columnas
numéricas. `SUM` ignora NULL (no afecta el total), pero para evitar que ese
NULL se propague en cálculos posteriores conviene aplicar `coalesce` **antes**
de agregar:

```python
df_eval = df_joined.withColumn("monto", F.coalesce(F.col("monto"), F.lit(0.0)))
res = df_eval.groupBy("segmento").agg(F.sum("monto").alias("monto_total"))
```

`AVG` ignora NULL de forma distinta (promedia solo los no-nulos): reemplazar
por 0 ahí **sí cambia el resultado**. Aplicar `coalesce` a una columna que
alimenta un `AVG` solo si el negocio lo pide explícitamente — no por defecto.
Registrar la decisión con `INFO_COALESCE_APPLIED` o `WARN_COALESCE_APPLIED`
según corresponda (ver `warnings.md`).

## Limpieza semántica post-codegen

```python
def cleanup_redundant_vars(locals_dict: dict):
    '''Elimina variables DataFrame temporales no referenciadas tras codegen.
    Útil en modo lenient para no dejar ruido en el notebook generado.'''
    to_delete = [k for k, v in locals_dict.items() if isinstance(v, DataFrame) and k.startswith("df_temp_")]
    for k in to_delete:
        del locals_dict[k]
        print(f"[CLEANUP] Variable temporal eliminada: {k}")
```

Ejecutar antes de las secciones `QA`/`OUTPUTS` del notebook, nunca antes —
podría borrar un DataFrame que una transformación posterior todavía necesita.
