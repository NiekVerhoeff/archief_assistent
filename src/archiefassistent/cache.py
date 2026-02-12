from __future__ import annotations
import json, hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from .config import CACHE_DIR

def _ckey(model: str, file_sha: str, chunk_idx: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f"{model}|{file_sha}|{chunk_idx}".encode()).hexdigest()
    return CACHE_DIR / f"chunk_{key}.json"

def cache_load(model: str, file_sha: str, chunk_idx: int) -> Optional[Dict[str, Any]]:
    p = _ckey(model, file_sha, chunk_idx)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def cache_save(model: str, file_sha: str, chunk_idx: int, data: Dict[str, Any]) -> None:
    p = _ckey(model, file_sha, chunk_idx)
    try:
        p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
