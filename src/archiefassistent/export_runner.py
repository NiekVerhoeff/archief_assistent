from __future__ import annotations
import json
from typing import Any, Dict, List, Optional

def apply_transform(value: Any, transform: Optional[str]) -> Any:
    if transform in (None, "", "(none)"):
        return value

    if transform == "strip":
        return value.strip() if isinstance(value, str) else value

    if transform == "first":
        if isinstance(value, list):
            return value[0] if value else None
        return value

    if transform.startswith("join:"):
        sep = transform.split(":", 1)[1]
        if isinstance(value, list):
            return sep.join(str(x) for x in value if x is not None)
        return value

    return value

def map_record(record: Dict[str, Any], mapping: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for tgt_key, spec in (mapping or {}).items():
        if isinstance(spec, dict):
            src = spec.get("source")
            tx = spec.get("transform")
        else:
            src = spec
            tx = None

        if not src:
            continue

        val = record.get(src)
        out[tgt_key] = apply_transform(val, tx)
    return out
