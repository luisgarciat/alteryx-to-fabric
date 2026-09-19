# Onboarding — Migración Alteryx → Microsoft Fabric

Skill de Claude que migra workflows Alteryx (.yxmd/.yxzp) a notebooks
PySpark para Microsoft Fabric. Ya está probado contra un flujo real de
producción y publicado para toda la organización — la mayoría no necesita
instalar nada.

## 1) Confirma que ya lo tienes

Pregúntale a tu Claude: **"¿qué skills tienes disponibles?"** Si aparece
`alteryx-to-fabric`, listo — salta al paso 3. Está publicado en
Configuración de admin → **Habilidades**, así que le llega solo a todo el
equipo, sin que nadie tenga que instalarlo a mano.

Si no aparece, avísale a tu admin — puede que el skill se haya
despublicado, o que tu cuenta no esté en la organización correcta.

## 2) Si te aparecen DOS "alteryx-to-fabric"

Pasó al menos una vez: alguien se instaló una copia **personal** antes de
que se publicara la de **organización**, y quedan las dos a la vez. Borra
la que instalaste tú mismo (la personal) y deja solo la de organización —
esa es la que se mantiene consistente para todo el equipo.

## 3) Úsalo

Sin comandos especiales — solo pídele a Claude lo que necesites, por
ejemplo:

> "Tengo este archivo ventas.yxmd, ayúdame a migrarlo a Fabric"

El skill se activa solo. **La primera vez que lo uses en un lugar nuevo
puede tardar un poco más** — internamente localiza o clona el repositorio
público (`github.com/luisgarciat/alteryx-to-fabric`) para trabajar siempre
con la versión más actualizada, no una copia congelada. Es normal, no está
trabado.

Si quieres adelantar ese paso, clónalo tú antes (el repo es público, no
pide login ni permisos):
```bash
git clone https://github.com/luisgarciat/alteryx-to-fabric.git
```

## Si usas Claude Code y quieres el modo de proyecto

Con el repo clonado, ábrelo como carpeta de trabajo en Claude Code (Open
Folder, o `claude` desde terminal parado ahí) — ahí el skill se activa
directo desde `.claude/skills/alteryx-to-fabric/`, sin pasar por la versión
de cuenta. Es la misma skill; esta ruta es más rápida si de por sí trabajas
en esa carpeta.

## Si encuentras algo que la skill no traduce bien

Es normal — cada workflow real expone casos nuevos. Dile a Claude qué
encontraste; probablemente se arregla en `references/tool_mapping.md` o en
`scripts/` del repo, con una prueba de regresión agregada. El que lo
arregle sube el cambio con `git push` — el resto del equipo lo recibe solo,
en su siguiente uso del skill (no hace falta reinstalar nada, salvo que
cambie el skill de cuenta mismo, algo raro).
