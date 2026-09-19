# Caso — 02_join_summarize (ejemplo sintético)

**Fecha:** 2026-09-18 · **Modo:** lenient

Este NO es un workflow real — es el fixture `fixtures/02_join_summarize.yxmd`
del propio repo, usado aquí solo para mostrar el formato de reporte. Bórralo
en cuanto tengas el primer caso real acumulado.

## Qué se migró

Un flujo simple: dos `Input Data` → `Join` → `Summarize` → `Output Data` (5
nodos en total).

## Qué costó trabajo

Nada — los 5 nodos ya tenían generador propio. Único aviso: el `how` del
Join se infirió automáticamente como `inner` (`DECISION_JOIN_MODE_INFERRED`)
porque la única salida conectada aguas abajo era `Join`.

## Resultado

Notebook generado sin TODOs pendientes.

## Sugerencia para la skill

Ninguna — caso limpio, sirve como piso de referencia (no como ejemplo de
un caso con problemas reales que resolver).
