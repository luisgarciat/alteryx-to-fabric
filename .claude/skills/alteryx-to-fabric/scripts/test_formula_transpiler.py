"""
Pruebas de regresión de formula_transpiler.py.

Incluye tanto los casos de diseño original como los que salieron de correr
la skill contra un workflow Alteryx real de producción (ver el historial de
warnings que motivó cada caso — el operador IN, Null() como función, unidades
de hora en DateTimeAdd, DateTimeHour, IsEmpty y nombres de campo con punto no
estaban cubiertos hasta que un archivo real los expuso).

Correr con: pytest scripts/test_formula_transpiler.py -v
"""

import pytest
from formula_transpiler import transpile


def _compiles(code: str) -> bool:
    """Valida que el codigo generado sea Python sintacticamente valido incluso
    embebido en una linea completa (ver el bug de comentarios inline que se
    corrigio en la Fase 3: un '#' a mitad de expresion se come el resto de la
    linea, incluyendo parentesis de cierre)."""
    compile(f"df = df.withColumn('campo', {code})", "<test>", "exec")
    return True


def test_iif_basico():
    r = transpile('IIF([monto]>100,"alto","bajo")')
    assert not r.warnings
    assert _compiles(r.code)
    assert r.code == "F.when((F.col('monto')) > (F.lit(100)), F.lit('alto')).otherwise(F.lit('bajo'))"


def test_if_elseif_else_endif():
    r = transpile('IF [x]<9 THEN "bajo" ELSEIF [x]>20 THEN "alto" ELSE "medio" ENDIF')
    assert not r.warnings
    assert _compiles(r.code)
    assert r.code.startswith("F.when(")
    assert ".otherwise(" in r.code


def test_row_reference_con_ventana():
    r = transpile("[Row-1:monto]+[monto]", window_var="w_31")
    assert _compiles(r.code)
    assert "F.lag('monto', 1).over(w_31)" in r.code


def test_row_reference_sin_ventana_advierte():
    r = transpile("[Row-1:monto]+[monto]")
    codes = {w["code"] for w in r.warnings}
    assert "WARN_ROW_REF_WITHOUT_WINDOW" in codes


def test_concatenacion_de_strings():
    r = transpile('[nombre]+" "+[apellido]')
    assert not r.warnings
    assert _compiles(r.code)
    assert r.code.count("F.concat(") == 2


def test_plus_ambiguo_entre_campos_advierte():
    r = transpile("[a]+[b]")
    codes = {w["code"] for w in r.warnings}
    assert "WARN_AMBIGUOUS_PLUS" in codes


def test_regex_replace():
    r = transpile('REGEX_Replace([email], "@.*$", "")')
    assert not r.warnings
    assert _compiles(r.code)
    assert r.code == "F.regexp_replace(F.col('email'), '@.*$', '')"


def test_datetimeparse_traduce_formato_strftime():
    r = transpile('DateTimeParse([fecha_str], "%m-%d-%Y")')
    assert not r.warnings
    assert "F.to_date(F.col('fecha_str'), 'MM-dd-yyyy')" == r.code


def test_expresion_malformada_no_lanza_excepcion():
    r = transpile("[a] +")
    assert r.code == "F.lit(None)"
    assert any(w["code"] == "WARN_PARSE_ERROR" for w in r.warnings)


# --- Casos que salieron de un workflow Alteryx real (ver samples/) ---------


def test_operador_in_con_literales():
    r = transpile('[PRIORIDAD] IN ("3","4","5")')
    assert not r.warnings
    assert _compiles(r.code)
    assert r.code == "(F.col('PRIORIDAD')).isin('3', '4', '5')"


def test_operador_in_multilinea_con_muchos_valores():
    """Reproduce el caso real: IN con una lista larga partida en varias lineas."""
    expr = '[NIVEL] IN (\n"A",\n"B",\n"C"\n)'
    r = transpile(expr)
    assert not r.warnings
    assert _compiles(r.code)
    assert ".isin(" in r.code


def test_null_como_funcion_con_parentesis():
    """Null() es una llamada a funcion en Alteryx real, no un literal sin
    parentesis -- este caso rompia el parser (se comia el Null pero dejaba
    '()' sueltos, tumbando el IF/ENDIF que lo contenia)."""
    r = transpile("IF [x]<9 THEN Null() ELSE [x] ENDIF")
    assert not r.warnings
    assert _compiles(r.code)
    assert "F.lit(None)" in r.code


def test_null_sin_parentesis_sigue_funcionando():
    r = transpile("IF [x]<9 THEN Null ELSE [x] ENDIF")
    assert _compiles(r.code)
    assert "F.lit(None)" in r.code


def test_datetimeadd_con_horas():
    r = transpile('DateTimeAdd([ts], -6, "hours")')
    assert not r.warnings
    assert _compiles(r.code)
    assert "3600" in r.code and ".cast('timestamp')" in r.code


def test_datetimehour():
    r = transpile("DateTimeHour([ts])")
    assert not r.warnings
    assert r.code == "F.hour(F.col('ts'))"


def test_isempty():
    r = transpile("IsEmpty([campo])")
    assert not r.warnings
    assert _compiles(r.code)


def test_campo_con_punto_se_escapa_con_backticks():
    """[User.SEM] sin backticks se interpreta como acceso a struct anidado en
    Spark -- un campo Alteryx con punto en el nombre no es eso."""
    r = transpile("[User.SEM]")
    assert r.code == "F.col('`User.SEM`')"
    assert _compiles(r.code)


def test_condicion_compleja_real_isempty_or_isnull():
    r = transpile('IF IsEmpty([FUENTE]) OR IsNull([FUENTE]) THEN "OTROS" ELSE [FUENTE] ENDIF')
    assert not r.warnings
    assert _compiles(r.code)
