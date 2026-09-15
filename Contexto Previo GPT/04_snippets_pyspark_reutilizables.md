
# 04 — Snippets PySpark Reutilizables (Fabric Runtime 1.3+) — v0.2.0
📅 Fecha: 2025-10-30
👤 Autor: GPT de Migración Alteryx → PySpark (Fabric)
🔖 Estado: Actualizado (MVP v0.2.0) con `auto_cast_for_sort()` y `cleanup_redundant_vars()`

> Bloques de código listos para usar por el generador (codegen) al convertir desde Alteryx.  
> Convenciones: `F` = `pyspark.sql.functions`, `W` = `pyspark.sql.window.Window`, `df` = `DataFrame`.

---

## 0️⃣ Importaciones estándar
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from pyspark.sql import DataFrame
```

---

## 1️⃣ Lectura y Escritura

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

---

## 2️⃣ Joins

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

def repartition_on(df: DataFrame, keys: list[str], num_parts: int | None = None) -> DataFrame:
    return df.repartition(num_parts if num_parts else df.rdd.getNumPartitions(), *[F.col(k) for k in keys])

def broadcast_join(df_left: DataFrame, df_small: DataFrame, keys: list[str], how: str = 'inner') -> DataFrame:
    return df_left.join(F.broadcast(df_small), on=keys, how=how)
```

---

## 3️⃣ Transformaciones y Fórmulas

```python
def make_category(df: DataFrame, src_col: str = 'value') -> DataFrame:
    return df.withColumn(
        'category',
        F.when(F.col(src_col) >= 90, 'A')
         .when(F.col(src_col) >= 70, 'B')
         .when(F.col(src_col).isNull(), None)
         .otherwise('C')
    )

def to_date_col(df: DataFrame, src: str, fmt: str, dst: str = 'dt') -> DataFrame:
    return df.withColumn(dst, F.to_date(F.col(src), fmt))

def to_timestamp_col(df: DataFrame, src: str, fmt: str, dst: str = 'ts') -> DataFrame:
    return df.withColumn(dst, F.to_timestamp(F.col(src), fmt))

def fill_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    return df.na.fill(fill_map)

def drop_nulls(df: DataFrame, subset: list[str] | None = None) -> DataFrame:
    return df.na.drop(subset=subset)
```

### 3.4 Cast automático para columnas de ordenación o agrupación (Nuevo en v0.2.0)
```python
def auto_cast_for_sort(df: DataFrame, cols: list[str]) -> DataFrame:
    '''
    Detecta columnas usadas en ordenaciones y aplica cast a numérico si contienen solo dígitos.
    '''
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

---

## 4️⃣ Regex

```python
def regex_extract_col(df: DataFrame, src: str, pattern: str, group_idx: int, dst: str) -> DataFrame:
    return df.withColumn(dst, F.regexp_extract(F.col(src), pattern, group_idx))

def regex_replace_col(df: DataFrame, src: str, pattern: str, replacement: str, dst: str | None = None) -> DataFrame:
    col = F.regexp_replace(F.col(src), pattern, replacement)
    return df.withColumn(dst if dst else src, col)

def regex_count_matches(df: DataFrame, src: str, pattern: str, dst: str = 'match_count') -> DataFrame:
    return df.withColumn(dst, F.size(F.split(F.col(src), pattern)) - 1)
```

---

## 5️⃣ Agregaciones

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

---

## 6️⃣ Pivot / CrossTab

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

---

## 7️⃣ Limpieza de nulos y redundancia (Nuevo bloque v0.2.0)

```python
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

## 8️⃣ Cierre

Este archivo fue ampliado para incluir funciones de **auto-casteo**, **limpieza semántica**, y mensajes de **logging mejorado**, alineados con el refinamiento operativo v0.2.0.  
Estas funciones deben ejecutarse antes de las secciones `QA` o `OUTPUTS` del notebook generado.

**Versión:** MVP v0.2.0  
**Compatibilidad:** Fabric Runtime 1.3+  
**Autor:** GPT de Migración Alteryx → PySpark (Fabric)
