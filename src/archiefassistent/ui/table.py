from __future__ import annotations
import json
from typing import Dict, Any, List

TECH_FIELDS = ["path", "filename", "extension", "size_bytes", "sha256"]

def flatten_record_for_table(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(rec)
    tech = out.pop("technical", None) or {}
    if isinstance(tech, dict):
        for k in TECH_FIELDS:
            out[f"technical_{k}"] = tech.get(k)
    else:
        for k in TECH_FIELDS:
            out[f"technical_{k}"] = None

    # subjects: store as JSON string for editing
    subs = out.get("subjects")
    if isinstance(subs, list):
        out["subjects"] = json.dumps(subs, ensure_ascii=False)
    elif subs is None:
        out["subjects"] = "[]"
    else:
        out["subjects"] = json.dumps([str(subs)], ensure_ascii=False)
    return out

def unflatten_record_from_table(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: v for k, v in row.items() if not k.startswith("technical_")}

    tech = {}
    for k in TECH_FIELDS:
        tech[k] = row.get(f"technical_{k}")
    out["technical"] = tech

    # subjects back to list
    subs = out.get("subjects")
    if isinstance(subs, str):
        try:
            out["subjects"] = json.loads(subs)
        except Exception:
            out["subjects"] = [subs] if subs else []
    elif subs is None:
        out["subjects"] = []
    return out
