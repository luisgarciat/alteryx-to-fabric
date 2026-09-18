#!/usr/bin/env python3
"""
Transpilador del lenguaje Formula de Alteryx a expresiones PySpark.

Es un mini-compilador de verdad (tokenizer -> parser recursivo -> AST ->
codegen), no reemplazos de texto con regex: el lenguaje Formula tiene
precedencia de operadores, anidamiento de funciones y un IF/ENDIF que puede
usarse como valor, y una traducción por regex se rompe apenas hay paréntesis
anidados o un string con un operador adentro.

Ver references/formula_language.md para la tabla de equivalencias en la que
se basa este archivo, y references/warnings.md para el significado de cada
código WARN_* que puede emitir.

Uso como librería:
    from formula_transpiler import transpile
    r = transpile('IIF([monto]>100,"alto","bajo")')
    print(r.code)       # F.when((F.col('monto')) > (F.lit(100)), F.lit('alto')).otherwise(F.lit('bajo'))
    print(r.warnings)    # []

Para Multi-Row Formula (usa [Row-1:Campo] / [Row+1:Campo]), pasar el nombre
de la variable Window que el generador de notebook (Fase 4) va a definir:
    transpile('[Row-1:monto]+[monto]', window_var='w_23')
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# Tokenizer
# --------------------------------------------------------------------------

_TOKEN_SPEC = [
    ("NUMBER", r"\d+\.\d+|\d+"),
    ("STRING", r'"(?:[^"]|"")*"'),
    ("FIELD", r"\[[^\]]+\]"),
    ("NE1", r"<>"),
    ("NE2", r"!="),
    ("EQ", r"=="),
    ("LE", r"<="),
    ("GE", r">="),
    ("LT", r"<"),
    ("GT", r">"),
    ("ASSIGN", r"="),
    ("PLUS", r"\+"),
    ("MINUS", r"-"),
    ("STAR", r"\*"),
    ("SLASH", r"/"),
    ("PERCENT", r"%"),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("COMMA", r","),
    ("IDENT", r"[A-Za-z_][A-Za-z_0-9]*"),
    ("WS", r"\s+"),
    ("MISMATCH", r"."),
]
_TOKEN_RE = re.compile("|".join(f"(?P<{name}>{pat})" for name, pat in _TOKEN_SPEC))

_FIELD_ROW_RE = re.compile(r"^Row([+-]\d+):(.+)$")

_COMPARISON_TYPES = {"EQ", "ASSIGN", "NE1", "NE2", "LT", "LE", "GT", "GE"}


class ParseError(Exception):
    pass


@dataclass
class Token:
    type: str
    value: str


def tokenize(expr: str) -> list[Token]:
    tokens = []
    for m in _TOKEN_RE.finditer(expr):
        kind = m.lastgroup
        value = m.group()
        if kind == "WS":
            continue
        if kind == "MISMATCH":
            raise ParseError(f"Caracter inesperado {value!r} en posicion {m.start()} de: {expr!r}")
        tokens.append(Token(kind, value))
    tokens.append(Token("EOF", ""))
    return tokens


# --------------------------------------------------------------------------
# AST
# --------------------------------------------------------------------------


@dataclass
class Num:
    value: float
    is_int: bool


@dataclass
class Str:
    value: str


@dataclass
class NullLit:
    pass


@dataclass
class Field:
    name: str
    row_offset: int = 0


@dataclass
class Call:
    name: str
    args: list


@dataclass
class BinOp:
    op: str
    left: object
    right: object


@dataclass
class UnaryOp:
    op: str
    operand: object


@dataclass
class IfExpr:
    branches: list  # [(cond, then), ...]
    else_expr: object | None


@dataclass
class InExpr:
    value: object
    items: list


# --------------------------------------------------------------------------
# Parser (descendente recursivo, precedencia estilo Alteryx/SQL)
#   OR > AND > NOT > comparacion > + - > * / % > unario > primario
# --------------------------------------------------------------------------


class Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def advance(self) -> Token:
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def at_keyword(self, kw: str) -> bool:
        t = self.peek()
        return t.type == "IDENT" and t.value.upper() == kw

    def expect_keyword(self, kw: str) -> None:
        if not self.at_keyword(kw):
            raise ParseError(f"Se esperaba '{kw}', se encontro {self.peek()!r}")
        self.advance()

    def expect(self, type_: str) -> Token:
        t = self.peek()
        if t.type != type_:
            raise ParseError(f"Se esperaba {type_}, se encontro {t!r}")
        return self.advance()

    def parse(self):
        node = self.parse_or()
        if self.peek().type != "EOF":
            raise ParseError(f"Token inesperado al final de la expresion: {self.peek()!r}")
        return node

    def parse_or(self):
        left = self.parse_and()
        while self.at_keyword("OR"):
            self.advance()
            left = BinOp("OR", left, self.parse_and())
        return left

    def parse_and(self):
        left = self.parse_not()
        while self.at_keyword("AND"):
            self.advance()
            left = BinOp("AND", left, self.parse_not())
        return left

    def parse_not(self):
        if self.at_keyword("NOT"):
            self.advance()
            return UnaryOp("NOT", self.parse_not())
        return self.parse_comparison()

    def parse_comparison(self):
        left = self.parse_additive()
        if self.at_keyword("IN"):
            self.advance()
            self.expect("LPAREN")
            items = []
            if self.peek().type != "RPAREN":
                items.append(self.parse_or())
                while self.peek().type == "COMMA":
                    self.advance()
                    items.append(self.parse_or())
            self.expect("RPAREN")
            return InExpr(left, items)
        if self.peek().type in _COMPARISON_TYPES:
            op = self.advance().type
            right = self.parse_additive()
            return BinOp(op, left, right)
        return left

    def parse_additive(self):
        left = self.parse_multiplicative()
        while self.peek().type in ("PLUS", "MINUS"):
            op = self.advance().type
            left = BinOp(op, left, self.parse_multiplicative())
        return left

    def parse_multiplicative(self):
        left = self.parse_unary()
        while self.peek().type in ("STAR", "SLASH", "PERCENT"):
            op = self.advance().type
            left = BinOp(op, left, self.parse_unary())
        return left

    def parse_unary(self):
        if self.peek().type in ("PLUS", "MINUS"):
            op = self.advance().type
            return UnaryOp(op, self.parse_unary())
        return self.parse_primary()

    def parse_primary(self):
        t = self.peek()
        if t.type == "NUMBER":
            self.advance()
            return Num(float(t.value), "." not in t.value)
        if t.type == "STRING":
            self.advance()
            return Str(t.value[1:-1].replace('""', '"'))
        if t.type == "FIELD":
            self.advance()
            content = t.value[1:-1]
            m = _FIELD_ROW_RE.match(content)
            if m:
                return Field(m.group(2), int(m.group(1)))
            return Field(content, 0)
        if t.type == "LPAREN":
            self.advance()
            node = self.parse_or()
            self.expect("RPAREN")
            return node
        if t.type == "IDENT":
            upper = t.value.upper()
            if upper == "IF":
                return self.parse_if()
            self.advance()
            if self.peek().type == "LPAREN":
                self.advance()
                args = []
                if self.peek().type != "RPAREN":
                    args.append(self.parse_or())
                    while self.peek().type == "COMMA":
                        self.advance()
                        args.append(self.parse_or())
                self.expect("RPAREN")
                return Call(t.value, args)
            if upper == "NULL":
                # Alteryx normalmente invoca Null() como funcion (con parentesis);
                # el caso sin parentesis se soporta igual como literal de respaldo.
                return NullLit()
            return Call(t.value, [])
        raise ParseError(f"Token inesperado: {t!r}")

    def parse_if(self):
        self.advance()  # IF
        cond = self.parse_or()
        self.expect_keyword("THEN")
        then = self.parse_or()
        branches = [(cond, then)]
        while self.at_keyword("ELSEIF"):
            self.advance()
            cond2 = self.parse_or()
            self.expect_keyword("THEN")
            branches.append((cond2, self.parse_or()))
        else_expr = None
        if self.at_keyword("ELSE"):
            self.advance()
            else_expr = self.parse_or()
        self.expect_keyword("ENDIF")
        return IfExpr(branches, else_expr)


# --------------------------------------------------------------------------
# Traduccion de tokens de formato de fecha: strftime (Alteryx) -> java.time (Spark)
# --------------------------------------------------------------------------

_DATE_TOKEN_MAP = {
    "%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd",
    "%H": "HH", "%I": "hh", "%M": "mm", "%S": "ss",
    "%p": "a", "%A": "EEEE", "%a": "EEE", "%B": "MMMM",
    "%b": "MMM", "%j": "DDD", "%z": "XX",
}


def translate_date_format(fmt: str) -> tuple[str, list[str]]:
    out, unknown = [], []
    i = 0
    while i < len(fmt):
        if fmt[i] == "%" and i + 1 < len(fmt):
            token = fmt[i : i + 2]
            out.append(_DATE_TOKEN_MAP.get(token, token))
            if token not in _DATE_TOKEN_MAP:
                unknown.append(token)
            i += 2
        else:
            out.append(fmt[i])
            i += 1
    return "".join(out), unknown


# --------------------------------------------------------------------------
# Codegen
# --------------------------------------------------------------------------

_STRING_RETURNING_FUNCS = {
    "trim", "trimleft", "trimright", "uppercase", "lowercase", "padleft",
    "padright", "substring", "replace", "regex_replace", "tostring", "left", "right",
}
_NUMBER_RETURNING_FUNCS = {
    "round", "ceiling", "floor", "abs", "mod", "tonumber", "length", "regex_countmatches",
}


def is_stringish(node) -> bool:
    if isinstance(node, Str):
        return True
    if isinstance(node, Call):
        return node.name.lower() in _STRING_RETURNING_FUNCS
    if isinstance(node, BinOp) and node.op == "PLUS":
        return is_stringish(node.left) or is_stringish(node.right)
    return False


def is_numberish(node) -> bool:
    if isinstance(node, Num):
        return True
    if isinstance(node, Call):
        return node.name.lower() in _NUMBER_RETURNING_FUNCS
    if isinstance(node, BinOp) and node.op in ("PLUS", "MINUS", "STAR", "SLASH", "PERCENT"):
        return True
    return False


class Codegen:
    def __init__(self, window_var: str | None = None):
        self.window_var = window_var
        self.warnings: list[dict] = []

    def warn(self, code: str, message: str) -> None:
        self.warnings.append({"code": code, "message": message})

    def gen(self, node) -> str:
        method = getattr(self, f"gen_{type(node).__name__}", None)
        if method is None:
            self.warn("WARN_UNSUPPORTED_EXPRESSION", f"Nodo AST sin generador: {type(node).__name__}")
            return "F.lit(None)"
        return method(node)

    def gen_Num(self, node: Num) -> str:
        return f"F.lit({int(node.value) if node.is_int else node.value})"

    def gen_Str(self, node: Str) -> str:
        return f"F.lit({node.value!r})"

    def gen_NullLit(self, node: NullLit) -> str:
        return "F.lit(None)"

    def gen_Field(self, node: Field) -> str:
        # Un nombre de campo con '.' se interpreta como acceso a struct anidado
        # en F.col()/F.lag()/F.lead() a menos que se delimite con backticks.
        name = f"`{node.name}`" if "." in node.name else node.name
        if node.row_offset == 0:
            return f"F.col({name!r})"
        if self.window_var is None:
            self.warn(
                "WARN_ROW_REF_WITHOUT_WINDOW",
                f"Referencia [Row{node.row_offset:+d}:{node.name}] sin ventana asociada "
                "(pasa window_var= al transpilar un Multi-Row Formula).",
            )
            return "F.lit(None)"
        fn = "F.lag" if node.row_offset < 0 else "F.lead"
        return f"{fn}({name!r}, {abs(node.row_offset)}).over({self.window_var})"

    def gen_InExpr(self, node: InExpr) -> str:
        all_literal = all(isinstance(it, (Num, Str)) for it in node.items)
        if all_literal:
            vals = [str(int(it.value) if it.is_int else it.value) if isinstance(it, Num) else repr(it.value) for it in node.items]
            return f"({self.gen(node.value)}).isin({', '.join(vals)})"
        self.warn("WARN_NON_LITERAL_ARG", "IN con elementos no literales; se genero como cadena de comparaciones OR.")
        return "(" + " | ".join(f"(({self.gen(node.value)}) == ({self.gen(it)}))" for it in node.items) + ")"

    def gen_UnaryOp(self, node: UnaryOp) -> str:
        inner = self.gen(node.operand)
        if node.op == "NOT":
            return f"~({inner})"
        if node.op == "MINUS":
            return f"-({inner})"
        return inner

    def gen_BinOp(self, node: BinOp) -> str:
        op = node.op
        l, r = node.left, node.right
        if op == "AND":
            return f"({self.gen(l)}) & ({self.gen(r)})"
        if op == "OR":
            return f"({self.gen(l)}) | ({self.gen(r)})"
        if op in ("EQ", "ASSIGN"):
            return f"({self.gen(l)}) == ({self.gen(r)})"
        if op in ("NE1", "NE2"):
            return f"({self.gen(l)}) != ({self.gen(r)})"
        if op == "LT":
            return f"({self.gen(l)}) < ({self.gen(r)})"
        if op == "LE":
            return f"({self.gen(l)}) <= ({self.gen(r)})"
        if op == "GT":
            return f"({self.gen(l)}) > ({self.gen(r)})"
        if op == "GE":
            return f"({self.gen(l)}) >= ({self.gen(r)})"
        if op == "PLUS":
            if is_stringish(l) or is_stringish(r):
                return f"F.concat({self.gen(l)}, {self.gen(r)})"
            if not (is_numberish(l) or is_numberish(r)):
                self.warn(
                    "WARN_AMBIGUOUS_PLUS",
                    "No se pudo determinar si '+' es concatenacion de texto o suma numerica "
                    "(ningun operando tiene tipo conocido); se genero como suma. Revisar el "
                    "tipo real de los campos en el Select/Input de origen.",
                )
            return f"({self.gen(l)}) + ({self.gen(r)})"
        if op == "MINUS":
            return f"({self.gen(l)}) - ({self.gen(r)})"
        if op == "STAR":
            return f"({self.gen(l)}) * ({self.gen(r)})"
        if op == "SLASH":
            return f"({self.gen(l)}) / ({self.gen(r)})"
        if op == "PERCENT":
            return f"({self.gen(l)}) % ({self.gen(r)})"
        raise ParseError(f"Operador no soportado en codegen: {op}")

    def gen_IfExpr(self, node: IfExpr) -> str:
        code = "F" + "".join(f".when({self.gen(c)}, {self.gen(t)})" for c, t in node.branches)
        if node.else_expr is not None:
            code += f".otherwise({self.gen(node.else_expr)})"
        return code

    def gen_Call(self, node: Call) -> str:
        handler = _FUNCTION_HANDLERS.get(node.name.lower())
        if handler is None:
            self.warn("WARN_UNSUPPORTED_FUNCTION", f"Funcion Alteryx sin equivalencia registrada: {node.name}(...)")
            args_code = ", ".join(self.gen(a) for a in node.args)
            return f"_unsupported_function({node.name!r}, [{args_code}])"
        return handler(self, node.args)


def _lit_str_arg(cg: Codegen, arg) -> str:
    """Argumentos que deben ser literal Python (patrones regex, nombres de unidad),
    no una Column. Si no es un string literal, no se puede resolver en tiempo de
    generacion: se marca para revision."""
    if isinstance(arg, Str):
        return repr(arg.value)
    cg.warn("WARN_NON_LITERAL_ARG", "Se esperaba un literal de texto en este argumento; revisar manualmente.")
    return cg.gen(arg)


def _h_iif(cg: Codegen, args):
    if len(args) != 3:
        cg.warn("WARN_ARITY_MISMATCH", f"IIF espera 3 argumentos, recibio {len(args)}.")
    padded = (args + [NullLit(), NullLit(), NullLit()])[:3]
    cond, a, b = padded
    return f"F.when({cg.gen(cond)}, {cg.gen(a)}).otherwise({cg.gen(b)})"


def _h_isnull(cg: Codegen, args):
    return f"({cg.gen(args[0])}).isNull()"


def _h_left(cg: Codegen, args):
    return f"F.substring({cg.gen(args[0])}, 1, {cg.gen(args[1])})"


def _h_right(cg: Codegen, args):
    n = cg.gen(args[1])
    return f"F.substring({cg.gen(args[0])}, -({n}), {n})"


def _h_substring(cg: Codegen, args):
    start_node = args[1]
    start_code = f"F.lit({int(start_node.value) + 1})" if isinstance(start_node, Num) else f"({cg.gen(start_node)}) + 1"
    return f"F.substring({cg.gen(args[0])}, {start_code}, {cg.gen(args[2])})"


def _h_replace(cg: Codegen, args):
    if isinstance(args[1], Str):
        return f"F.regexp_replace({cg.gen(args[0])}, {re.escape(args[1].value)!r}, {cg.gen(args[2])})"
    cg.warn("WARN_NON_LITERAL_ARG", "Replace() con texto de busqueda no literal; revisar el escape manualmente.")
    return f"F.regexp_replace({cg.gen(args[0])}, {cg.gen(args[1])}, {cg.gen(args[2])})"


_DATEADD_SECONDS = {
    "second": 1, "seconds": 1, "minute": 60, "minutes": 60, "hour": 3600, "hours": 3600,
}


def _h_datetimeadd(cg: Codegen, args):
    fecha, n, unidad = args[0], args[1], args[2]
    unit = unidad.value.lower() if isinstance(unidad, Str) else None
    if unit in ("day", "days"):
        return f"F.date_add({cg.gen(fecha)}, {cg.gen(n)})"
    if unit in ("month", "months"):
        return f"F.add_months({cg.gen(fecha)}, {cg.gen(n)})"
    if unit in ("year", "years"):
        n_code = f"{int(n.value) * 12}" if isinstance(n, Num) else f"({cg.gen(n)}) * 12"
        return f"F.add_months({cg.gen(fecha)}, {n_code})"
    if unit in _DATEADD_SECONDS:
        # Sin funcion generica de "sumar N segundos" en Spark: castear a epoch,
        # sumar, castear de vuelta a timestamp.
        return f"(({cg.gen(fecha)}).cast('long') + ({cg.gen(n)}) * {_DATEADD_SECONDS[unit]}).cast('timestamp')"
    cg.warn("WARN_UNSUPPORTED_DATE_UNIT", f"Unidad de DateTimeAdd no reconocida: {unidad!r} (se esperaba days/months/years/hours/minutes/seconds literal).")
    return f"F.date_add({cg.gen(fecha)}, {cg.gen(n)})"


def _h_datetimediff(cg: Codegen, args):
    a, b = args[0], args[1]
    unit = args[2].value.lower() if len(args) > 2 and isinstance(args[2], Str) else "days"
    if unit == "days":
        return f"F.datediff({cg.gen(a)}, {cg.gen(b)})"
    if unit in ("months", "years"):
        expr = f"F.months_between({cg.gen(a)}, {cg.gen(b)})"
        if unit == "years":
            expr = f"({expr}) / 12"
        cg.warn("INFO_DATEDIFF_APPROX", "DateTimeDiff en meses/anios usa months_between; redondear segun el negocio.")
        return expr
    cg.warn("WARN_UNSUPPORTED_DATE_UNIT", "Unidad de DateTimeDiff no reconocida; se genero datediff en dias.")
    return f"F.datediff({cg.gen(a)}, {cg.gen(b)})"


def _h_date_fmt(spark_fn: str):
    def handler(cg: Codegen, args):
        fmt_node = args[1]
        if isinstance(fmt_node, Str):
            spark_fmt, unknown = translate_date_format(fmt_node.value)
            if unknown:
                cg.warn("WARN_UNSUPPORTED_DATE_TOKEN", f"Token(s) sin mapeo conocido {unknown} en formato {fmt_node.value!r}.")
            return f"{spark_fn}({cg.gen(args[0])}, {spark_fmt!r})"
        cg.warn("WARN_NON_LITERAL_ARG", "Formato de fecha no literal; no se pudo traducir el patron automaticamente.")
        return f"{spark_fn}({cg.gen(args[0])}, {cg.gen(fmt_node)})"

    return handler


_FUNCTION_HANDLERS = {
    "iif": _h_iif,
    "isnull": _h_isnull,
    "trim": lambda cg, a: f"F.trim({cg.gen(a[0])})",
    "trimleft": lambda cg, a: f"F.ltrim({cg.gen(a[0])})",
    "trimright": lambda cg, a: f"F.rtrim({cg.gen(a[0])})",
    "length": lambda cg, a: f"F.length({cg.gen(a[0])})",
    "uppercase": lambda cg, a: f"F.upper({cg.gen(a[0])})",
    "lowercase": lambda cg, a: f"F.lower({cg.gen(a[0])})",
    "left": _h_left,
    "right": _h_right,
    "padleft": lambda cg, a: f"F.lpad({cg.gen(a[0])}, {cg.gen(a[1])}, {_lit_str_arg(cg, a[2])})",
    "padright": lambda cg, a: f"F.rpad({cg.gen(a[0])}, {cg.gen(a[1])}, {_lit_str_arg(cg, a[2])})",
    "substring": _h_substring,
    "replace": _h_replace,
    "regex_replace": lambda cg, a: f"F.regexp_replace({cg.gen(a[0])}, {_lit_str_arg(cg, a[1])}, {_lit_str_arg(cg, a[2])})",
    "regex_match": lambda cg, a: f"({cg.gen(a[0])}).rlike({_lit_str_arg(cg, a[1])})",
    "regex_countmatches": lambda cg, a: f"F.size(F.split({cg.gen(a[0])}, {_lit_str_arg(cg, a[1])})) - F.lit(1)",
    "contains": lambda cg, a: f"({cg.gen(a[0])}).contains({cg.gen(a[1])})",
    "startswith": lambda cg, a: f"({cg.gen(a[0])}).startswith({cg.gen(a[1])})",
    "endswith": lambda cg, a: f"({cg.gen(a[0])}).endswith({cg.gen(a[1])})",
    "tostring": lambda cg, a: f"({cg.gen(a[0])}).cast('string')",
    "tonumber": lambda cg, a: f"({cg.gen(a[0])}).cast('double')",
    "round": lambda cg, a: f"F.round({cg.gen(a[0])}, {cg.gen(a[1])})" if len(a) > 1 else f"F.round({cg.gen(a[0])})",
    "ceiling": lambda cg, a: f"F.ceil({cg.gen(a[0])})",
    "floor": lambda cg, a: f"F.floor({cg.gen(a[0])})",
    "abs": lambda cg, a: f"F.abs({cg.gen(a[0])})",
    "mod": lambda cg, a: f"({cg.gen(a[0])}) % ({cg.gen(a[1])})",
    "datetimetoday": lambda cg, a: "F.current_date()",
    "datetimenow": lambda cg, a: "F.current_timestamp()",
    "datetimeadd": _h_datetimeadd,
    "datetimediff": _h_datetimediff,
    "datetimeparse": _h_date_fmt("F.to_date"),
    "datetimeformat": _h_date_fmt("F.date_format"),
    "datetimeyear": lambda cg, a: f"F.year({cg.gen(a[0])})",
    "datetimemonth": lambda cg, a: f"F.month({cg.gen(a[0])})",
    "datetimeday": lambda cg, a: f"F.dayofmonth({cg.gen(a[0])})",
    "datetimehour": lambda cg, a: f"F.hour({cg.gen(a[0])})",
    "datetimeminute": lambda cg, a: f"F.minute({cg.gen(a[0])})",
    "datetimesecond": lambda cg, a: f"F.second({cg.gen(a[0])})",
    "isempty": lambda cg, a: f"({cg.gen(a[0])}) == F.lit('')",
    "null": lambda cg, a: "F.lit(None)",
    "true": lambda cg, a: "F.lit(True)",
    "false": lambda cg, a: "F.lit(False)",
}


# --------------------------------------------------------------------------
# API publica
# --------------------------------------------------------------------------


@dataclass
class TranspileResult:
    code: str
    warnings: list = field(default_factory=list)


def transpile(expr: str, window_var: str | None = None) -> TranspileResult:
    try:
        ast_root = Parser(tokenize(expr)).parse()
    except ParseError as exc:
        return TranspileResult(
            code="F.lit(None)",
            warnings=[{"code": "WARN_PARSE_ERROR", "message": str(exc)}],
        )
    cg = Codegen(window_var=window_var)
    code = cg.gen(ast_root)
    return TranspileResult(code=code, warnings=cg.warnings)
