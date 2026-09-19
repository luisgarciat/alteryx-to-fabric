# Guía definitiva — Migración Alteryx → Microsoft Fabric

Todo lo que necesita instalar y hacer un integrante del equipo, en su propio
equipo, para que esto funcione. Sigue los pasos en orden.

## A) Requisitos en tu equipo (una sola vez)

1. **Git**
   - Windows: instala desde [git-scm.com/downloads](https://git-scm.com/downloads) (o usa GitHub Desktop, que ya lo trae incluido).
   - Verifica: `git --version`

2. **Python 3.9 o superior**
   - Instala desde [python.org/downloads](https://www.python.org/downloads/)
   - **Importante en Windows:** marca la casilla "Add python.exe to PATH" durante la instalación — si se te pasa, el resto de los pasos falla con "python no se reconoce como un comando".
   - Verifica: `python --version`
   - Opcional, solo si vas a correr las pruebas de paridad (Fase 5): `pip install pytest`

Sin estos dos, el skill no puede clonar el repo ni ejecutar sus scripts —
son la base de todo lo demás.

## B) Confirma que ya tienes el skill

3. Pregúntale a tu Claude: **"¿qué skills tienes disponibles?"**
   - Si aparece `alteryx-to-fabric` → ya está, no instalas nada más. Está
     publicado para toda la organización desde Configuración de admin →
     Habilidades, así que llega solo.
   - Si **no aparece** → avisa a tu admin; puede que se haya despublicado o
     que tu cuenta no esté en la organización correcta.
   - Si te aparecen **dos** (`alteryx-to-fabric` repetida) → alguien
     (quizás tú) instaló una copia personal antes de que se publicara la de
     organización. Borra la personal y deja solo la de organización.

## C) Opcional — clona el repositorio de antemano

4. El repo es público, no pide login ni permisos:
   ```bash
   git clone https://github.com/luisgarciat/alteryx-to-fabric.git
   ```
   Si te saltas este paso no pasa nada: el skill lo clona él solo la
   primera vez que lo uses — solo que esa primera vez tarda un poco más
   (está descargando el repo en segundo plano, no está trabado).

## D) Úsalo

5. Sin comandos especiales — pídele a Claude lo que necesites, por ejemplo:
   > "Tengo este archivo ventas.yxmd, ayúdame a migrarlo a Fabric"

   El skill sigue 5 fases (inventario → parser → transpilador de
   expresiones → generación de notebook → validación de paridad) y decide
   solo en cuál entrar según lo que le pidas.

## E) Opcional — modo "de proyecto" en Claude Code

6. Si usas la app de Claude Code (no Cowork) y ya clonaste el repo (paso
   C), puedes abrir esa carpeta como directorio de trabajo (Open Folder, o
   `claude` desde una terminal parada ahí). Ahí el skill se activa directo
   desde `.claude/skills/alteryx-to-fabric/`, sin pasar por la versión de
   cuenta — es la misma skill, solo un poco más directo si de por sí
   trabajas en esa carpeta.

## Problemas comunes

| Síntoma | Qué hacer |
|---|---|
| Tarda mucho la primera vez que la usas | Normal — está clonando el repo. No está trabada. |
| Aparecen dos "alteryx-to-fabric" | Borra la que instalaste tú mismo (personal); deja la de organización. |
| No aparece en "¿qué skills tienes disponibles?" | Avisa al admin — revisa que siga publicada en Habilidades. |
| `python` o `git` "no se reconoce como un comando" | No quedó en el PATH — reinstala marcando esa opción, o agrégalo a mano a las variables de entorno de Windows. |
| Algo de tu workflow no se traduce bien | Es normal, cada flujo real expone casos nuevos. Dile a Claude qué encontraste — probablemente se arregla en `references/` o `scripts/` del repo. Quien lo arregle sube el cambio (`git push`) y el resto del equipo lo recibe solo, en su siguiente uso — no hace falta reinstalar nada. |
