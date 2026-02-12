from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# -----------------------
# Schema/type helpers
# -----------------------

def _schema_type(prop_schema: Dict[str, Any]) -> str:
    t = (prop_schema or {}).get("type")
    if isinstance(t, list):
        nn = [x for x in t if x != "null"]
        return nn[0] if nn else "string"
    return t or "string"

def _schema_items_schema(prop_schema: Dict[str, Any]) -> Dict[str, Any]:
    items = (prop_schema or {}).get("items")
    return items if isinstance(items, dict) else {}

def _schema_required_keys(schema: Dict[str, Any]) -> List[str]:
    req = (schema or {}).get("required")
    return req if isinstance(req, list) else []

def _schema_properties(schema: Dict[str, Any]) -> Dict[str, Any]:
    props = (schema or {}).get("properties")
    return props if isinstance(props, dict) else {}

def _first_non_empty(values: List[Any]) -> Any:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, dict) and len(v) == 0:
            continue
        return v
    return None

# -----------------------
# Merge primitives
# -----------------------

def _merge_strings(values: List[Any], mode: str = "first", max_len: int = 1600) -> Optional[str]:
    ss = [v.strip() for v in values if isinstance(v, str) and v.strip()]
    if not ss:
        return None
    if mode == "concat":
        unique, seen = [], set()
        for s in ss:
            key = s[:120]
            if key not in seen:
                seen.add(key)
                unique.append(s)
        out = "\n\n".join(unique)
        return out[:max_len]
    return ss[0]

def _merge_numbers(values: List[Any], mode: str = "first") -> Optional[float]:
    nums: List[float] = []
    for v in values:
        if isinstance(v, (int, float)):
            nums.append(float(v))
        elif isinstance(v, str):
            try:
                nums.append(float(v.strip()))
            except Exception:
                pass
    if not nums:
        return None
    if mode == "max":
        return max(nums)
    if mode == "min":
        return min(nums)
    return nums[0]

def _deep_merge_objects(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge where existing non-empty values in 'a' win, but we fill gaps from 'b'.
    Dicts merge recursively.
    """
    out = dict(a)
    for k, v in b.items():
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v
            continue
        if isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_objects(out[k], v)
    return out

def _merge_objects(values: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for v in values:
        if not isinstance(v, dict):
            continue
        out = _deep_merge_objects(out, v)
    return out

# -----------------------
# Schema-driven object identity
# -----------------------

_ID_HINT_RE = re.compile(
    r"(?:^|_)(id|identifier|identificatie|uuid|guid|uri|iri|url|ref|reference|nummer|number|code|key)(?:$|_)",
    re.IGNORECASE,
)

def _norm_scalar(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bool):
        return "true" if x else "false"
    return str(x).strip().lower()

def _is_simple_type(s: Dict[str, Any]) -> bool:
    t = _schema_type(s)
    return t in ("string", "integer", "number", "boolean")

def _derive_identity_keys(items_schema: Dict[str, Any]) -> List[str]:
    """
    Pick a good set of keys to build a signature for items in an array-of-objects,
    based purely on schema.

    Priority:
    1) keys that *look like* identifiers by name
    2) required keys (if any) that are simple types
    3) otherwise: up to 4 simple-type keys (stable-ish)
    """
    props = _schema_properties(items_schema)
    if not props:
        return []

    # 1) identifier-like keys
    id_like = [k for k in props.keys() if _ID_HINT_RE.search(k)]
    # keep only simple types (string/int/number/bool) since they serialize well
    id_like = [k for k in id_like if _is_simple_type(props.get(k, {}))]
    if id_like:
        return id_like[:2]  # usually one is enough, but allow 2

    # 2) required keys that are simple types
    req = _schema_required_keys(items_schema)
    req_simple = [k for k in req if k in props and _is_simple_type(props.get(k, {}))]
    if req_simple:
        return req_simple[:4]

    # 3) fallback: choose a few simple-type properties
    simple_keys = [k for k, s in props.items() if isinstance(s, dict) and _is_simple_type(s)]
    return simple_keys[:4]

def _object_signature(obj: Dict[str, Any], keys: List[str]) -> str:
    """
    Create a stable signature from chosen keys.
    If keys are empty or all missing, fallback to canonical JSON.
    """
    parts: List[str] = []
    for k in keys:
        if k in obj and obj.get(k) not in (None, "", [], {}):
            parts.append(f"{k}={_norm_scalar(obj.get(k))}")

    if parts:
        return "sig:" + "|".join(parts)

    # fallback: canonical json
    try:
        return "json:" + json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "str:" + str(obj)

# -----------------------
# Array merge (schema-driven)
# -----------------------

def _coerce_to_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, dict):
        return [v]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        # allow JSON text
        if s.startswith("[") or s.startswith("{"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
                if isinstance(parsed, dict):
                    return [parsed]
            except Exception:
                pass
        return [s]
    return []

def _merge_array_of_objects(values: List[Any], items_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Merge lists of dicts from multiple chunks:
    - dedupe by schema-derived signature keys
    - deep-merge duplicates to fill missing subfields
    """
    keys = _derive_identity_keys(items_schema)

    merged_by_sig: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    for v in values:
        for item in _coerce_to_list(v):
            if not isinstance(item, dict):
                continue
            sig = _object_signature(item, keys)
            if sig not in merged_by_sig:
                merged_by_sig[sig] = item
                order.append(sig)
            else:
                merged_by_sig[sig] = _deep_merge_objects(merged_by_sig[sig], item)

    return [merged_by_sig[s] for s in order]

def _merge_array_of_scalars(values: List[Any]) -> List[Any]:
    out: List[Any] = []
    seen = set()

    for v in values:
        for item in _coerce_to_list(v):
            if item is None:
                continue
            key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
            if key in seen:
                continue
            seen.add(key)
            out.append(item)

    return out

def _merge_arrays(values: List[Any], items_schema: Dict[str, Any]) -> List[Any]:
    """
    If items are objects (per schema), merge as objects; otherwise merge as scalars.
    """
    if _schema_type(items_schema) == "object":
        return _merge_array_of_objects(values, items_schema)
    return _merge_array_of_scalars(values)

# -----------------------
# Main aggregator
# -----------------------

def aggregate_chunk_dicts(
    chunk_dicts: List[Dict[str, Any]],
    schema: Dict[str, Any],
    *,
    technical: Optional[Dict[str, Any]] = None,
    filetype_guess: Optional[str] = None,
) -> Dict[str, Any]:
    if not chunk_dicts:
        base: Dict[str, Any] = {}
        props: Dict[str, Any] = _schema_properties(schema)
        if "technical" in props and technical is not None:
            base["technical"] = technical
        if "filetype" in props and filetype_guess is not None:
            base["filetype"] = filetype_guess
        return base

    props: Dict[str, Any] = _schema_properties(schema)
    out: Dict[str, Any] = {}

    for key, prop_schema in props.items():
        prop_schema = prop_schema if isinstance(prop_schema, dict) else {}
        values = [d.get(key) for d in chunk_dicts]
        t = _schema_type(prop_schema)

        if t == "string":
            merge_mode = "concat" if key in ("description", "samenvatting", "abstract", "notes") else "first"
            out[key] = _merge_strings(values, mode=merge_mode)

        elif t == "array":
            items_schema = _schema_items_schema(prop_schema)
            out[key] = _merge_arrays(values, items_schema)

        elif t == "object":
            out[key] = _merge_objects(values)

        elif t in ("integer", "number"):
            out[key] = _merge_numbers(values, mode="first")
            if t == "integer" and isinstance(out[key], float):
                try:
                    out[key] = int(out[key])
                except Exception:
                    out[key] = None

        elif t == "boolean":
            out[key] = _first_non_empty(values)
            if isinstance(out[key], str):
                s = out[key].strip().lower()
                if s in ("true", "ja", "yes", "1"):
                    out[key] = True
                elif s in ("false", "nee", "no", "0"):
                    out[key] = False
                else:
                    out[key] = None

        else:
            out[key] = _first_non_empty(values)

    # Ensure technical + filetype if schema expects them
    if "technical" in props and (not isinstance(out.get("technical"), dict)) and technical is not None:
        out["technical"] = technical
    if "filetype" in props and (not out.get("filetype")) and filetype_guess is not None:
        out["filetype"] = filetype_guess

    # Drop extras not in schema (optional)
    out = {k: v for k, v in out.items() if k in props}
    return out
