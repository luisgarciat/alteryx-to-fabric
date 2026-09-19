# Casos de migración

Un subfolder por migración real, generado por la skill al final de la Fase
4/5 (ver "Fase 6 — Reporte de caso" en
[`SKILL.md`](../.claude/skills/alteryx-to-fabric/SKILL.md)). Sirve para
acumular evidencia real de qué le falta a la skill, en vez de adivinar —
así se encontraron y arreglaron los bugs reales que tiene hoy.

## Convención

```
cases/<AAAA-MM-DD>_<nombre-del-workflow>/
  reporte.json   -- estructurado, mismo shape siempre
  REPORTE.md     -- lo mismo en prosa, con sugerencias de mejora
```

## Cómo se usa

- La skill lo genera sola al terminar una migración (o cuando confirmes
  éxito en la conversación), y te pregunta antes de subirlo — puede tener
  detalles reales del negocio, así que decides tú si sube tal cual.
- Para ver patrones entre todos los casos acumulados (qué herramienta sin
  soporte se repite más, qué warning aparece más seguido):
  ```bash
  python .claude/skills/alteryx-to-fabric/scripts/summarize_cases.py
  ```

`2026-09-18_ejemplo-join-summarize/` es un ejemplo sintético (corrido sobre
un fixture de prueba, no un workflow real) para mostrar el formato — bórralo
cuando tengas casos reales acumulados.
