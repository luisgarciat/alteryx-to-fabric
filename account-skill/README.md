# alteryx-to-fabric.skill — loader de cuenta

Este archivo es para quien **no** pueda usar el skill de proyecto normal
(`.claude/skills/alteryx-to-fabric/`, que se activa solo al abrir este repo
como carpeta de trabajo en Claude Code). Algunas interfaces (por ejemplo,
Cowork) solo reconocen skills instaladas a nivel de **cuenta**, no skills de
carpeta de proyecto — para esos casos existe este paquete.

## Qué es

Un skill de cuenta "delgado": no contiene la lógica real de migración. Cada
vez que se usa, primero localiza (o clona) este mismo repositorio en tu
máquina, corre `git pull` para asegurarse de tener la versión más reciente,
y luego lee y sigue el `SKILL.md` real dentro de
`.claude/skills/alteryx-to-fabric/`. Así, instalarlo una vez no te deja con
una copia congelada — cada uso jala el contenido actualizado.

**Importante:** esto actualiza sola la *lógica de migración* (herramientas
soportadas, scripts, referencias) porque vive en este repo. Si en algún
momento cambia el *loader mismo* (por ejemplo la URL del repo o el texto
que dispara cuándo se activa), sí hace falta descargar la versión nueva de
este archivo y volver a instalarla — por eso este archivo se mantiene
versionado aquí, no se manda suelto por chat.

## Cómo instalarlo

1. Descarga `alteryx-to-fabric.skill` de esta carpeta (o clona el repo
   completo, que ya lo incluye).
2. Ábrelo en tu sesión de Claude/Cowork y usa el botón **"Save skill"** de
   la tarjeta que aparece — eso lo instala en tu cuenta.
3. Repite lo mismo cada integrante del equipo, en su propia cuenta.

## Prerequisito

El repositorio es privado. Para que el `git pull`/`git clone` automático
funcione sin pedir credenciales, tu máquina necesita tener acceso git
configurado a este repo de antemano (lo más simple: haberlo clonado al
menos una vez con GitHub Desktop o `git clone`, lo cual deja las
credenciales guardadas).
