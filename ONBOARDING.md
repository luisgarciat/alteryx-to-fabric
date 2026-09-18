# Onboarding — Migración Alteryx → Microsoft Fabric

Este repo trae un skill de Claude Code que acelera la migración de workflows
Alteryx (.yxmd/.yxzp) a notebooks PySpark para Microsoft Fabric. Ya está
armado y probado contra un flujo real de producción — no hay que construir
nada, solo clonar y usarlo.

## 1) Acepta la invitación de GitHub

Revisa tu correo o las notificaciones de GitHub — deberías tener una
invitación a `luisgarciat/alteryx-to-fabric`. Acéptala antes de seguir (sin
esto, el clone del paso 2 falla con "repository not found").

## 2) Clona el repositorio

```bash
git clone https://github.com/luisgarciat/alteryx-to-fabric.git
```

## 3) Confirma que tienes Python 3.9+

```bash
python --version
```

Si no lo tienes, instálalo desde [python.org](https://www.python.org/downloads/)
(marca "Add python.exe to PATH" durante la instalación en Windows). Las
Fases 1–4 del skill son solo librería estándar de Python, sin dependencias
externas.

Opcional, solo si vas a correr las pruebas de paridad de la Fase 5:
```bash
pip install pytest
```

## 4) Abre la carpeta con Claude Code

Abre la carpeta que acabas de clonar (`alteryx-to-fabric/`) con Claude Code
— con la app de escritorio (Open Folder) o con el comando `claude` desde una
terminal parada en esa carpeta.

**No hay que instalar ni activar el skill a mano.** Claude Code lo carga
solo desde `.claude/skills/alteryx-to-fabric/` en cuanto abres esa carpeta.
Solo pregúntale a Claude algo sobre Alteryx, un `.yxmd`, o migrar un flujo a
Fabric/Lakehouse, y el skill se activa.

## 5) Prueba rápida (opcional, para confirmar que todo funciona)

Desde la carpeta del repo:

```bash
cd .claude/skills/alteryx-to-fabric
python scripts/parse_workflow.py fixtures/02_join_summarize.yxmd --out /tmp/prueba.ir.json
python scripts/transpile_formula.py /tmp/prueba.ir.json
python scripts/generate_notebook.py /tmp/prueba.ir.json --modo lenient
```

Si ves `OK  notebook generado -> ...` al final, todo está en orden.

## Cómo usarlo con un workflow real

Solo dile a Claude algo como *"tengo este .yxmd, ayúdame a migrarlo a
Fabric"* y adjunta o indícale la ruta del archivo. El skill sigue 5 fases
(inventario → parser → transpilador de expresiones → generación de notebook
→ validación de paridad) — Claude sabe en cuál entrar según lo que pidas.
Más detalle en [`SKILL.md`](.claude/skills/alteryx-to-fabric/SKILL.md).

## Si encuentras algo que la skill no traduce bien

Es normal — cada workflow real expone casos nuevos (así se construyó y
mejoró hasta ahora). Dile a Claude qué encontraste; lo más probable es que
se pueda arreglar en `references/tool_mapping.md` o en los scripts de
`scripts/`, con una prueba de regresión agregada. Cuando mejores algo, sube
el cambio (`git add` / `commit` / `push`) para que el resto del equipo se
beneficie con su próximo `git pull`.
