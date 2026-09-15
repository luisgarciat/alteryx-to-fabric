"""
Helper para inferir el modo de Join (inner/left/right/outer) a partir del modelo intermedio JSON.
Asume esquema:
{
  "workflow": {
    "nodes": [{"id": "3", "tool": "Join", "name": "...", "inputs": [...], "outputs": [{"name":"J","to":"4"}, ...]}],
    "connections": [{"from": {"tool_id":"3","output":"J"}, "to": {"tool_id":"4","input":"I1"}}, ...]
  }
}
"""
from typing import Dict, Any, Optional, Set

def infer_join_mode(model: Dict[str, Any], join_tool_id: str) -> (str, dict):
    nodes = {n["id"]: n for n in model.get("workflow",{}).get("nodes",[])}
    conns = model.get("workflow",{}).get("connections",[])
    join = nodes.get(str(join_tool_id))
    reasons = {"rule": None, "union_tool_id": None, "outputs_detected": []}
    if not join or join.get("tool","").lower() != "join":
        return "inner", {**reasons, "rule":"not_join"}

    # 1) Explícito por outputs
    outs = [o.get("name") for o in join.get("outputs",[]) if "name" in o]
    outs_set = set(outs)
    reasons["outputs_detected"] = outs
    if outs_set:
        if outs_set == {"J"}:
            reasons["rule"]="explicit_only_J"; return "inner", reasons
        if "J" in outs_set and "L" in outs_set and "R" not in outs_set:
            reasons["rule"]="explicit_JL"; return "left", reasons
        if "J" in outs_set and "R" in outs_set and "L" not in outs_set:
            reasons["rule"]="explicit_JR"; return "right", reasons
        if {"J","L","R"}.issubset(outs_set):
            reasons["rule"]="explicit_JLR"; return "outer", reasons

    # 2) Heurística por Union aguas abajo
    # Construir mapa de edges desde JID
    j_edges = [c for c in conns if str(c.get("from",{}).get("tool_id")) == str(join_tool_id)]
    # detectar unions destino
    unions = {}
    for c in j_edges:
        to_id = str(c.get("to",{}).get("tool_id"))
        inp = c.get("to",{}).get("input")
        unions.setdefault(to_id, []).append(inp or "I?")

    # checar si un mismo destino recibe múltiples entradas del mismo Join
    for to_id, inputs in unions.items():
        # buscar el nodo destino
        node = nodes.get(to_id)
        if not node or node.get("tool","").lower() != "union":
            continue
        reasons["union_tool_id"] = to_id
        # si hay múltiples entradas desde J → hay combinación de salidas
        if len(inputs) >= 2:
            # si no conocemos nombres exactos, suponer J+L como caso más común
            reasons["rule"]="heur_union_multi_inputs"
            return "left", reasons

    # 3) Fallback
    reasons["rule"]="fallback_inner"
    return "inner", reasons
