# src/archiefassistent/jobs.py
from __future__ import annotations

from importlib.metadata import files
from pathlib import Path
from typing import Optional, Dict, Any

from .schemas import FileTechnical, ArchiveMetadata, model_to_dict
from .extraction import walk_files, extract_text, sha256_file
from .chunking import chunk_text
from .ollama_client import call_ollama_structured, DEFAULT_SCHEMA
from .aggregation import aggregate_chunk_dicts
from .db import (
    save_record,
    set_job_total_files,
    increment_job_files_done,
)


def process_job(
    job_id: int,
    root_dir: str,
    model_tag: str,
    *,
    chunk_size: int = 2200,
    chunk_overlap: int = 150,
    max_chunks: int = 5,
    timeout_s: int = 180,
    max_files: int = 200,
    schema: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Process exactly one job id: scan files, chunk, call model, aggregate, save records.
    IMPORTANT: This module does NOT start/own any background worker loop.
    A separate process (worker.py) should call this function.
    """
    schema = schema or DEFAULT_SCHEMA

    files = walk_files(Path(root_dir))
    files = files[: max(0, int(max_files))]
    set_job_total_files(job_id, len(files))

    for fp in files:
        print(f"[job {job_id}] processing file: {fp}")
        try:
            text = extract_text(fp)
            tech = FileTechnical(
                path=str(fp),
                filename=fp.name,
                extension=fp.suffix.lower(),
                size_bytes=fp.stat().st_size,
                sha256=sha256_file(fp),
            )
            filetype_guess = fp.suffix.lower().lstrip(".")

            # Empty/unextractable text: still store technical record
            if not text.strip():
                rec = ArchiveMetadata(technical=tech, filetype=filetype_guess)
                save_record(job_id, fp.name, model_to_dict(rec))
                increment_job_files_done(job_id)
                continue

            chunks = chunk_text(
                text,
                chunk_size=chunk_size,
                overlap=chunk_overlap,
                max_chunks=max_chunks,
            )
            #print(chunks)
            
            chunk_dicts = []
            for idx, ch in enumerate(chunks):
                try:
                    recd = call_ollama_structured(
                        model=model_tag,
                        content=ch,
                        schema=schema,          # <-- job schema
                        technical=tech,
                        timeout_s=timeout_s,
                    )
                    chunk_dicts.append(recd)
                except Exception as e:
                    print(f"Model failed on chunk {idx+1} of {fp.name}: {e}")

            merged = aggregate_chunk_dicts(
                chunk_dicts,
                schema=schema,
                technical=model_to_dict(tech),
                filetype_guess=filetype_guess
            )

            save_record(job_id, fp.name, merged)
            increment_job_files_done(job_id)

        except Exception as e:
            # Keep behavior: log and continue to next file
            print(f"Failed on {fp.name}: {e}")
