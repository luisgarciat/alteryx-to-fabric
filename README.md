# Migración Alteryx → Microsoft Fabric

Este repositorio existe para que el equipo comparta el skill de Claude Code
que acelera la migración de workflows Alteryx (.yxmd/.yxzp) a notebooks
PySpark en Microsoft Fabric.

## Cómo usarlo

1. Clona este repositorio.
2. Asegúrate de tener **Python 3.9+** en el PATH (las fases 1–4 son solo
   librería estándar; `pip install pytest` únicamente si vas a correr las
   pruebas de paridad de la fase 5).
3. Abre esta carpeta con Claude Code. El skill vive en
   [`.claude/skills/alteryx-to-fabric/`](.claude/skills/alteryx-to-fabric/SKILL.md)
   y Claude Code lo carga automáticamente — no hay que instalar ni invocar
   nada a mano. Basta con preguntar de forma natural sobre Alteryx, un
   `.yxmd`, o migrar un flujo a Fabric/Lakehouse.
4. Para ver qué hace el skill y cómo están organizadas sus 6 fases
   (inventario, parser, transpilador Formula, generación de notebook, QA de
   paridad, reporte de caso), lee su
   [`SKILL.md`](.claude/skills/alteryx-to-fabric/SKILL.md).

### Si tu Claude Code no muestra el skill solo

Algunas interfaces (ej. Cowork) solo reconocen skills instaladas a nivel de
cuenta, no skills de carpeta de proyecto como este — aunque tengas el repo
clonado y abierto, no se activa solo. Para esos casos, instala
[`account-skill/alteryx-to-fabric.skill`](account-skill/alteryx-to-fabric.skill)
en tu cuenta (ver el README de esa carpeta) — es un loader delgado que jala
este mismo repo y sigue su `SKILL.md` real en cada uso, así que se mantiene
igual de actualizado.

## Estructura del repositorio

```
.
├── .claude/skills/alteryx-to-fabric/   # el skill de proyecto (ver su propio README/SKILL.md)
├── account-skill/                      # version empaquetada para instalar a nivel de cuenta
├── cases/                              # reportes de migraciones reales (Fase 6) -- retro para mejorar la skill
└── Contexto Previo GPT/                # material fuente original (GPT de migracion v0.2.0)
    y Promt GPT.docx                    # del que se porto la metodologia a este skill
```

`cases/` es la evidencia real detrás de cada mejora a la skill — ver su
propio [README](cases/README.md). Como puede tener detalles reales del
negocio, revisa cada reporte antes de subirlo si el repo llegara a hacerse
público de nuevo.

La carpeta `Contexto Previo GPT/` es el material original de un asistente
GPT de migración previo — se conserva como referencia histórica porque
algunas notas dentro del skill (`references/tool_mapping.md`, etc.) citan
esos archivos por nombre. No hace falta tocarla para usar el skill.

## Mantenimiento

Cuando alguien mejore el skill (agregar una herramienta a
`tool_mapping.md`, arreglar el transpilador, etc.), el cambio se hace dentro
de `.claude/skills/alteryx-to-fabric/` y se sube con un commit normal — todo
el que haga `git pull` queda con la versión más reciente automáticamente.
