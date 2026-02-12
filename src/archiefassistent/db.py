from __future__ import annotations
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from .config import DB_PATH, CACHE_DIR

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn

def init_db() -> None:
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY,
        name TEXT,
        root_dir TEXT,
        model_tag TEXT,
        created_at TEXT,
        status TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY,
        job_id INTEGER,
        filename TEXT,
        record_json TEXT,
        created_at TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS preprocess_files (
        id INTEGER PRIMARY KEY,
        job_id INTEGER,
        filename TEXT,
        path TEXT,
        technical_json TEXT,
        created_at TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS preprocess_chunks (
        id INTEGER PRIMARY KEY,
        job_id INTEGER,
        preprocess_file_id INTEGER,
        filename TEXT,
        chunk_type TEXT,              -- 'summary' or 'embed'
        chunk_index INTEGER,
        start_char INTEGER,
        end_char INTEGER,
        chunk_text TEXT,
        embedding_json TEXT,          -- JSON array of floats or NULL
        created_at TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(id),
        FOREIGN KEY(preprocess_file_id) REFERENCES preprocess_files(id)
    )""")

        # Export profiles (SHACL) + job mappings
    cur.execute("""CREATE TABLE IF NOT EXISTS export_profiles (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,            -- "shacl"
        shacl_text TEXT NOT NULL,      -- uploaded SHACL content (ttl/rdfxml/etc)
        manifest_json TEXT NOT NULL,   -- extracted target fields list
        created_at TEXT NOT NULL
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS job_export_mappings (
        id INTEGER PRIMARY KEY,
        job_id INTEGER NOT NULL,
        profile_id INTEGER NOT NULL,
        mapping_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(job_id, profile_id),
        FOREIGN KEY(job_id) REFERENCES jobs(id),
        FOREIGN KEY(profile_id) REFERENCES export_profiles(id)
    )""")

    cur.execute("PRAGMA table_info(jobs)")
    existing = [row[1] for row in cur.fetchall()]
    if "total_files" not in existing:
        cur.execute("ALTER TABLE jobs ADD COLUMN total_files INTEGER DEFAULT 0")
    if "files_done" not in existing:
        cur.execute("ALTER TABLE jobs ADD COLUMN files_done INTEGER DEFAULT 0")

    if "options_json" not in existing:
        cur.execute("ALTER TABLE jobs ADD COLUMN options_json TEXT DEFAULT '{}'")

    conn.commit()
    conn.close()

def save_preprocess_file(job_id: int, filename: str, path: str, technical: Dict[str, Any]) -> int:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO preprocess_files (job_id, filename, path, technical_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (int(job_id), filename, path, json.dumps(technical, ensure_ascii=False), now),
    )
    file_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(file_id)

def save_preprocess_chunk(
    job_id: int,
    preprocess_file_id: int,
    filename: str,
    chunk_type: str,
    chunk_index: int,
    start_char: int,
    end_char: int,
    chunk_text: str,
    embedding: Optional[List[float]] = None,
) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    emb_json = json.dumps(embedding, ensure_ascii=False) if embedding is not None else None
    cur.execute(
        """INSERT INTO preprocess_chunks
           (job_id, preprocess_file_id, filename, chunk_type, chunk_index,
            start_char, end_char, chunk_text, embedding_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            int(job_id),
            int(preprocess_file_id),
            filename,
            chunk_type,
            int(chunk_index),
            int(start_char),
            int(end_char),
            chunk_text,
            emb_json,
            now,
        ),
    )
    conn.commit()
    conn.close()

def create_job(name: str, root_dir: str, model_tag: str, options: Optional[Dict[str, Any]] = None) -> int:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    options_json = json.dumps(options or {}, ensure_ascii=False)
    cur.execute(
        "INSERT INTO jobs (name, root_dir, model_tag, options_json, created_at, status) VALUES (?, ?, ?, ?, ?, ?)",
        (name, root_dir, model_tag, options_json, now, "queued")
    )
    job_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(job_id)

def update_job_status(job_id: int, status: str) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()
    conn.close()

def set_job_total_files(job_id: int, total: int) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET total_files = ? WHERE id = ?", (total, job_id))
    conn.commit()
    conn.close()

def increment_job_files_done(job_id: int) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET files_done = files_done + 1 WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()

def save_record(job_id: int, filename: str, record: Dict[str, Any]) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO records (job_id, filename, record_json, created_at) VALUES (?, ?, ?, ?)",
        (job_id, filename, json.dumps(record, ensure_ascii=False), now)
    )
    conn.commit()
    conn.close()

def list_jobs() -> List[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        try:
            d["options"] = json.loads(d.get("options_json") or "{}")
        except Exception:
            d["options"] = {}
        rows.append(d)
    conn.close()
    return rows

def get_job(job_id: int) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ? LIMIT 1", (int(job_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["options"] = json.loads(d.get("options_json") or "{}")
    except Exception:
        d["options"] = {}
    return d

def get_job_records(job_id: int) -> List[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, filename, record_json, created_at FROM records WHERE job_id = ? ORDER BY id", (job_id,))
    rows: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        try:
            rec = json.loads(r["record_json"])
        except Exception:
            rec = {}
        rec["__db_id"] = r["id"]
        rec["__filename"] = r["filename"]
        rec["__created_at"] = r["created_at"]
        rows.append(rec)
    conn.close()
    return rows

def update_record_db(record_id: int, new_record: Dict[str, Any]) -> None:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE records SET record_json = ? WHERE id = ?",
            (json.dumps(new_record, ensure_ascii=False), int(record_id))
        )
        conn.commit()
    finally:
        conn.close()

def get_next_queued_job() -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["options"] = json.loads(d.get("options_json") or "{}")
    except Exception:
        d["options"] = {}
    return d

def delete_job(job_id: int) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE job_id = ?", (job_id,))
    cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()

def create_export_profile(name: str, kind: str, shacl_text: str, manifest: List[Dict[str, Any]]) -> int:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO export_profiles (name, kind, shacl_text, manifest_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (name, kind, shacl_text, json.dumps(manifest, ensure_ascii=False), now),
    )
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    return int(pid)

def list_export_profiles(kind: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    if kind:
        cur.execute("SELECT * FROM export_profiles WHERE kind = ? ORDER BY created_at DESC", (kind,))
    else:
        cur.execute("SELECT * FROM export_profiles ORDER BY created_at DESC")
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        try:
            d["manifest"] = json.loads(d.get("manifest_json") or "[]")
        except Exception:
            d["manifest"] = []
        rows.append(d)
    conn.close()
    return rows

def get_export_profile(profile_id: int) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM export_profiles WHERE id = ? LIMIT 1", (int(profile_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["manifest"] = json.loads(d.get("manifest_json") or "[]")
    except Exception:
        d["manifest"] = []
    return d

def upsert_job_export_mapping(job_id: int, profile_id: int, mapping: Dict[str, Any]) -> None:
    conn = _get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    mapping_json = json.dumps(mapping or {}, ensure_ascii=False)

    # SQLite upsert
    cur.execute(
        """
        INSERT INTO job_export_mappings (job_id, profile_id, mapping_json, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(job_id, profile_id) DO UPDATE SET
            mapping_json=excluded.mapping_json,
            created_at=excluded.created_at
        """,
        (int(job_id), int(profile_id), mapping_json, now),
    )
    conn.commit()
    conn.close()

def get_job_export_mapping(job_id: int, profile_id: int) -> Dict[str, Any]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT mapping_json FROM job_export_mappings WHERE job_id = ? AND profile_id = ? LIMIT 1",
        (int(job_id), int(profile_id)),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    try:
        return json.loads(row["mapping_json"] or "{}")
    except Exception:
        return {}


# init on import (same behavior as your monolith)
init_db()
