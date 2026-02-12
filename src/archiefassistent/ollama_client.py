from __future__ import annotations
import json
import time
from typing import Dict, Any, Optional, List

import requests

from .config import OLLAMA_BASE, CACHE_DIR
from .schemas import ArchiveMetadata, FileTechnical, model_to_dict


# A sane default schema (your UI can still pass its own)
DEFAULT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {
            "type": ["string", "null"],
            "description": "Korte titel van het archiefstuk. Neem over uit documentkop of onderwerpregel.",
            "examples": ["Basisschool De Kleine — Inspectierapport (onaangekondigd bezoek)"]
        },
        "description": {"type": "string"},
        "creator": {
            "type": ["string", "null"],
            "description": "Naam van de maker of verantwoordelijke van het document.",
            "examples": ["Gemeente Amsterdam, Dienst Onderwijs"]
        },
        "date_start": {
            "type": ["string", "null"],
            "description": "Begindatum van het document. Gebruik ISO 8601: YYYY-MM-DD indien mogelijk.",
        },
        "date_end": {
            "type": ["string", "null"],
            "description": "Einddatum van het document. Gebruik ISO 8601: YYYY-MM-DD indien mogelijk.",
        },
        "subjects": {"type": "array", "items": {"type": "string"}},
        "language": {"type": "string"},
        "addresses": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                "street": {"type": ["string","null"]},
                "house_number": {"type": ["string","null"]},
                "postal_code": {"type": ["string","null"]},
                "city": {"type": ["string","null"]},
                "country": {"type": ["string","null"]}
                },
                "required": ["street","house_number","postal_code","city","country"]
            }
        },    
        "rights": {"type": "string"},
        "sensitivity": {
            "type": ["string", "null"],
            "description": "Classificeer de gevoeligheid van het document.",
            "enum": ["openbaar", "intern", "vertrouwelijk", "geheim", None]
        },
        "retention": {"type": "string"},
        "filetype": {"type": "string"},
        "technical": {"type": "object"},
    },
    "required": ["title", "description", "creator", "date_start", "date_end", "subjects", "language", "addresses", "rights", "sensitivity", "retention", "filetype", "technical"],
}


def ensure_ollama_ready(model: str, timeout_s: int = 8) -> bool:
    try:
        resp = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=int(timeout_s))
        tags = resp.json()
        return any(t.get("name") == model for t in tags.get("models", []))
    except Exception:
        return False


def list_ollama_models(timeout_s: int = 8) -> List[str]:
    try:
        resp = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=int(timeout_s))
        tags = resp.json()
        return [m.get("name") for m in tags.get("models", []) if m.get("name")]
    except Exception:
        return []

def _ollama_embed(text: str, model: str, timeout_s: int = 60, retries: int = 3) -> Optional[List[float]]:
    """
    Try Ollama embedding endpoints. Returns list[float] or None.
    Supports common Ollama API variants:
      - POST /api/embeddings  {"model": "...", "prompt": "..."}
      - POST /api/embed       {"model": "...", "input": "..."}
    """
    backoff = 2
    last_err: Optional[Exception] = None

    payloads = [
        (f"{OLLAMA_BASE}/api/embeddings", {"model": model, "prompt": text}),
        (f"{OLLAMA_BASE}/api/embed", {"model": model, "input": text}),
    ]

    for _ in range(max(1, int(retries))):
        for url, payload in payloads:
            try:
                r = requests.post(url, json=payload, timeout=int(timeout_s))
                r.raise_for_status()
                data = r.json()

                # common shapes:
                # {"embedding":[...]} or {"embeddings":[[...]]}
                if isinstance(data.get("embedding"), list):
                    return data["embedding"]
                embs = data.get("embeddings")
                if isinstance(embs, list) and embs and isinstance(embs[0], list):
                    return embs[0]
                if isinstance(data.get("data"), list) and data["data"]:
                    # OpenAI-ish: {"data":[{"embedding":[...]}]}
                    e = data["data"][0].get("embedding")
                    if isinstance(e, list):
                        return e

            except Exception as e:
                last_err = e
                continue

        time.sleep(backoff)
        backoff = min(backoff * 2, 8)

    # Optional: log last_err somewhere
    return None

def _ollama_generate(payload: Dict[str, Any], timeout_s: int, retries: int = 3) -> Dict[str, Any]:
    backoff = 2
    last_err: Optional[Exception] = None
    for _ in range(max(1, int(retries))):
        try:
            r = requests.post(f"{OLLAMA_BASE}/api/generate", json=payload, timeout=int(timeout_s))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 8)
    raise last_err  # type: ignore

def _loose_json_extract(s: str) -> Dict[str, Any]:
    if not s:
        return {}
    s = s.strip()
    if s.startswith("```"):
        s = s.strip("`").replace("json\n", "").replace("json\r\n", "")

    spans = []
    bstart = s.find("{")
    if bstart != -1:
        bend = s.rfind("}")
        if bend > bstart:
            spans.append((bstart, bend + 1))

    for st, ed in spans:
        try:
            obj = json.loads(s[st:ed])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
    
def call_ollama_structured(
    model: str,
    content: str,
    schema: Dict[str, Any],
    technical: FileTechnical,
    *,
    timeout_s: int = 180,
    num_predict: int = 800,
    num_ctx: int = 2048,
) -> Dict[str, Any]:
    """
    Call Ollama with a JSON schema and return a dict that conforms to that schema.
    Fully schema-driven. No ArchiveMetadata assumptions.
    """

    prompt = f"""
    Je bent de Archiefassistent, een tool om archiefmedewerkers te helpen archiefstukken beter te beschrijven.
    Extraheer metadata uit de tekst hieronder.

    REGELS:
    - Antwoord UITSLUITEND in valide JSON
    - Gebruik alleen velden die in het schema voorkomen
    - Waarden altijd in het Nederlands
    - Onbekend of twijfelachtig → null of lege waarde
    - Geen commentaar, geen uitleg

    TEKST:
    {content}
    """.strip()

    payload = {
        "model": model,
        "format": schema,          # Ollama schema-constrained generation
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "repeat_penalty": 1.05,
            "top_k": 40,
            "top_p": 0.9,
            "num_predict": int(num_predict),
            "num_ctx": int(num_ctx),
        },
    }

    data = _ollama_generate(payload, timeout_s=timeout_s, retries=3)
    print(data)
    raw = (data.get("response") or "").strip()

    # --- Parse JSON ---
    try:
        obj = json.loads(raw) if raw else {}
    except Exception:
        obj = _loose_json_extract(raw)

    if not isinstance(obj, dict):
        obj = {}

    # --- Repair pass if model ignored schema ---
    if not obj and raw:
        prompt2 = f"""
        De assistent gaf ongeldige JSON terug.
        EXTRACTEER EN GEEF ALLEEN valide JSON terug dat overeenkomt met het schema.

        SCHEMA:
        {json.dumps(schema, ensure_ascii=False, indent=2)}

        TEKST:
        {raw}
        """.strip()

        follow = {
            "model": model,
            "format": schema,
            "prompt": prompt2,
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": min(int(num_predict), 400),
                "num_ctx": int(num_ctx),
            },
        }

        try:
            data2 = _ollama_generate(follow, timeout_s=min(60, timeout_s), retries=2)
            raw2 = (data2.get("response") or "").strip()
            obj = json.loads(raw2) if raw2 else _loose_json_extract(raw2)
        except Exception:
            obj = {}

    # --- Debug logging if still broken ---
    if not isinstance(obj, dict) or not obj:
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            dbg = CACHE_DIR / "ollama_raw_responses.log"
            with open(dbg, "a", encoding="utf-8") as fh:
                fh.write("\n\n====\n")
                fh.write(f"MODEL: {model}\n")
                fh.write("RAW_RESPONSE:\n")
                fh.write(raw + "\n")
                fh.write("FULL_RESPONSE:\n")
                fh.write(json.dumps(data, ensure_ascii=False, indent=2))
                fh.write("\n")
        except Exception:
            pass
        obj = {}

    # --- Schema-aware normalization ---
    props = schema.get("properties") or {}
    normalized: Dict[str, Any] = {}

    def schema_type(s: Dict[str, Any]) -> str:
        t = s.get("type")
        if isinstance(t, list):
            nn = [x for x in t if x != "null"]
            return nn[0] if nn else "string"
        return t or "string"

    for key, prop_schema in props.items():
        val = obj.get(key)
        t = schema_type(prop_schema if isinstance(prop_schema, dict) else {})

        if val is None:
            normalized[key] = None
            continue

        # Array normalization
        if t == "array":
            if isinstance(val, list):
                normalized[key] = val
            elif isinstance(val, str) and val.strip():
                normalized[key] = [val.strip()]
            else:
                normalized[key] = []

        # Object normalization
        elif t == "object":
            normalized[key] = val if isinstance(val, dict) else {}

        # Boolean normalization
        elif t == "boolean":
            if isinstance(val, bool):
                normalized[key] = val
            elif isinstance(val, str):
                v = val.strip().lower()
                normalized[key] = v in ("true", "ja", "yes", "1")
            else:
                normalized[key] = None

        # Number / integer
        elif t in ("number", "integer"):
            try:
                n = float(val)
                normalized[key] = int(n) if t == "integer" else n
            except Exception:
                normalized[key] = None

        # String (default)
        else:
            normalized[key] = str(val).strip() if isinstance(val, str) else val

    # --- Inject technical/filetype ONLY if schema expects them ---
    if "technical" in props and not isinstance(normalized.get("technical"), dict):
        normalized["technical"] = model_to_dict(technical)

    if "filetype" in props and not normalized.get("filetype"):
        normalized["filetype"] = technical.extension.lstrip(".") if technical else None

    return normalized


def generate_json_schema(
    model: str,
    description: str,
    *,
    timeout_s: int = 60,
    num_predict: int = 800,
    num_ctx: int = 2048,
) -> Dict[str, Any]:
    """
    Generate a JSON Schema for archival metadata extraction from a free-text description.

    Returns a dict JSON Schema (root object) suitable to pass as `format` to Ollama.
    """
    desc = (description or "").strip()
    if not desc:
        return DEFAULT_SCHEMA

    prompt = f"""
    Je bent een assistent die JSON Schema's maakt voor metadata-extractie door een LLM.

    Taak:
    - Maak een JSON Schema (root type=object) dat de gewenste metadata beschrijft.
    - Output is ALLEEN valide JSON (geen markdown, geen uitleg).
    - Zet "additionalProperties": false.
    - Gebruik per veld een duidelijke "description" (in het Nederlands).
    - Gebruik waar relevant nullability: bv. ["string","null"].
    - Voor classificaties: gebruik "enum" met toegestane waarden.
    - Voor arrays: gebruik {{ "type": "array", "items": ... }} en bij voorkeur "uniqueItems": true.
    - Voeg "required" toe zodat alle top-level velden altijd aanwezig zijn.

    Beschrijving van de gewenste metadata:
    {desc}
    """.strip()

    payload = {
        "model": model,
        "format": "json",   # for schema-gen we just need a JSON object back
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": int(num_predict),
            "num_ctx": int(num_ctx),
        },
    }

    data = _ollama_generate(payload, timeout_s=int(timeout_s), retries=3)
    raw = (data.get("response") or "").strip()

    # First attempt: strict parse, fallback to loose extraction
    try:
        obj = json.loads(raw) if raw else {}
    except Exception:
        obj = _loose_json_extract(raw)

    # Repair attempt if invalid / empty
    if not isinstance(obj, dict) or not obj:
        prompt2 = f"""
        De volgende output was geen valide JSON Schema.
        Geef ALLEEN een valide JSON Schema terug (root type=object) zonder uitleg.

        OUTPUT:
        {raw}

        HERINNERING:
        - Root: {{ "type": "object", "properties": {{...}} }}
        - "additionalProperties": false
        - Voeg "required" toe voor top-level velden
        """.strip()

        follow = {
            "model": model,
            "format": "json",
            "prompt": prompt2,
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": int(min(max(400, num_predict), 1200)),
                "num_ctx": int(num_ctx),
            },
        }
        data2 = _ollama_generate(follow, timeout_s=min(60, int(timeout_s)), retries=2)
        raw2 = (data2.get("response") or "").strip()
        try:
            obj = json.loads(raw2) if raw2 else {}
        except Exception:
            obj = _loose_json_extract(raw2)

    # Final validation + normalization
    if not isinstance(obj, dict):
        return DEFAULT_SCHEMA

    # Minimal sanity checks
    if obj.get("type") != "object" or not isinstance(obj.get("properties"), dict):
        # sometimes the model nests schema under a key
        # attempt to locate the first dict that looks like a schema
        candidate = None
        for v in obj.values():
            if isinstance(v, dict) and v.get("type") == "object" and isinstance(v.get("properties"), dict):
                candidate = v
                break
        if isinstance(candidate, dict):
            obj = candidate
        else:
            return DEFAULT_SCHEMA

    # Fill defaults that make downstream behavior predictable
    obj.setdefault("additionalProperties", False)

    props = obj.get("properties") or {}
    if isinstance(props, dict):
        # Ensure required exists and includes all top-level properties (optional preference)
        if "required" not in obj or not isinstance(obj.get("required"), list):
            obj["required"] = list(props.keys())

    return obj
