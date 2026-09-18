"""
Pruebas de regresión de parse_workflow.py, centradas en dos comportamientos
que salieron de correr la skill contra un workflow Alteryx real de
producción (10,600 líneas): el Filter en modo 'Simple' (no tiene <Expression>,
tiene una estructura Field/Operator/Operands aparte) y los Tool Container
deshabilitados (Alteryx no los ejecuta -- el parser tampoco debe traducirlos).

Correr con: pytest scripts/test_parse_workflow.py -v
"""

from xml.etree import ElementTree as ET

from formula_transpiler import transpile
from parse_workflow import collect_nodes, extract_filter


def _filter_cfg(inner_simple: str) -> ET.Element:
    return ET.fromstring(f"<Configuration><Mode>Simple</Mode><Simple>{inner_simple}</Simple></Configuration>")


def test_filter_simple_operador_comparacion_fecha_fija():
    cfg = _filter_cfg(
        "<Operator>&gt;=</Operator><Field>Fecha</Field>"
        "<Operands><DateType>fixed</DateType><Operand>2026-09-07</Operand></Operands>"
    )
    result = extract_filter(cfg)
    assert result["expression"] == '[Fecha] >= "2026-09-07"'


def test_filter_simple_today_sin_offset():
    cfg = _filter_cfg(
        "<Operator>=</Operator><Field>Fecha</Field>"
        "<Operands><DateType>today</DateType><PeriodCount>0</PeriodCount><Operand>2025-12-15</Operand></Operands>"
    )
    result = extract_filter(cfg)
    # Debe usar DateTimeToday() en vivo, NO el valor congelado en el XML
    # (ese Operand es solo el snapshot de cuando se guardo el archivo).
    assert "DateTimeToday()" in result["expression"]
    assert "2025-12-15" not in result["expression"]


def test_filter_simple_yesterday():
    cfg = _filter_cfg(
        "<Operator>!=</Operator><Field>Fecha</Field>"
        "<Operands><DateType>yesterday</DateType><Operand>2026-03-19</Operand></Operands>"
    )
    result = extract_filter(cfg)
    assert result["expression"] == '[Fecha] <> DateTimeAdd(DateTimeToday(), -1, "days")'
    # y que efectivamente transpile sin advertencias
    r = transpile(result["expression"])
    assert not r.warnings


def test_filter_simple_contains_y_notcontains():
    cfg = _filter_cfg("<Operator>NotContains</Operator><Field>NAMELIST</Field><Operands><Operand>P0</Operand></Operands>")
    result = extract_filter(cfg)
    assert result["expression"] == 'NOT Contains([NAMELIST], "P0")'


def test_filter_simple_isnotempty():
    cfg = _filter_cfg("<Operator>IsNotEmpty</Operator><Field>Telefono</Field><Operands></Operands>")
    result = extract_filter(cfg)
    assert result["expression"] == "NOT IsEmpty([Telefono])"
    r = transpile(result["expression"])
    assert not r.warnings


def test_filter_simple_isnull():
    cfg = _filter_cfg("<Operator>IsNull</Operator><Field>Telefono</Field><Operands></Operands>")
    result = extract_filter(cfg)
    assert result["expression"] == "ISNULL([Telefono])"


def test_filter_simple_operando_numerico_no_se_entrecomilla():
    cfg = _filter_cfg("<Operator>&gt;</Operator><Field>monto</Field><Operands><Operand>100</Operand></Operands>")
    result = extract_filter(cfg)
    assert result["expression"] == "[monto] > 100"


def test_filter_custom_sigue_funcionando():
    cfg = ET.fromstring("<Configuration><Expression>[a]&gt;1</Expression><Mode>Custom</Mode></Configuration>")
    result = extract_filter(cfg)
    assert result == {"expression": "[a]>1", "mode": "Custom"}


def test_filter_simple_date_type_no_reconocido_no_inventa_nada():
    """Un DateType raro (thisWeek, lastMonth, etc.) no se sintetiza -- mejor
    dejarlo vacio (y que se marque para revision manual) que adivinar mal."""
    cfg = _filter_cfg(
        "<Operator>=</Operator><Field>Fecha</Field>"
        "<Operands><DateType>thisWeek</DateType><Operand>2026-03-19</Operand></Operands>"
    )
    result = extract_filter(cfg)
    assert result["expression"] == ""


# --- Contenedores deshabilitados --------------------------------------------

_DISABLED_CONTAINER_XML = """
<Nodes>
  <Node ToolID="1">
    <GuiSettings Plugin="AlteryxGuiToolkit.ToolContainer.ToolContainer" />
    <Properties>
      <Configuration>
        <Caption>Apagado</Caption>
        <Disabled value="True" />
      </Configuration>
    </Properties>
    <ChildNodes>
      <Node ToolID="2">
        <GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
        <Properties><Configuration /></Properties>
        <EngineSettings />
      </Node>
    </ChildNodes>
  </Node>
  <Node ToolID="3">
    <GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
    <Properties><Configuration><SelectFields /></Configuration></Properties>
    <EngineSettings />
  </Node>
</Nodes>
"""


def test_contenedor_deshabilitado_excluye_todo_su_contenido():
    nodes_el = ET.fromstring(_DISABLED_CONTAINER_XML)
    out: dict = {}
    disabled_ids: list = []
    collect_nodes(nodes_el, out, disabled_ids)
    # ToolID 2 (el Join dentro del contenedor apagado) no debe aparecer
    assert "2" not in out
    # ToolID 3 (fuera del contenedor) si debe aparecer normal
    assert "3" in out
    assert "1" in disabled_ids
