# worker.py
import time

from src.archiefassistent.db import get_next_queued_job, update_job_status
from src.archiefassistent.jobs import process_job

POLL_SECONDS = 2

def _as_int(v, default):
    try:
        return int(v)
    except Exception:
        return int(default)


def main():
    print("Archiefassistent worker started.")
    try:
        while True:
            job = get_next_queued_job()
            if not job:
                time.sleep(POLL_SECONDS)
                continue

            job_id = int(job["id"])
            update_job_status(job_id, "running")
            print(f"[worker] running job {job_id}: {job.get('name')}")
            options = job.get("options") or {}

            timeout_s = _as_int(options.get("request_timeout"), 180)
            chunk_size = _as_int(options.get("chunk_size"), 2200)
            chunk_overlap = _as_int(options.get("chunk_overlap"), 150)
            max_chunks = _as_int(options.get("max_chunks"), 5)
            max_files = _as_int(options.get("max_files"), 200)
            schema = options.get("schema")

            try:
                process_job(job_id, job["root_dir"], job["model_tag"], timeout_s=timeout_s, chunk_size=chunk_size, chunk_overlap=chunk_overlap, max_chunks=max_chunks, max_files=max_files, schema=schema)
                update_job_status(job_id, "finished")
                print(f"[worker] finished job {job_id}")
            except Exception as e:
                update_job_status(job_id, f"failed: {e}")
                print(f"[worker] failed job {job_id}: {e}")

    except KeyboardInterrupt:
        print("\n[worker] stopping (Ctrl+C)")


if __name__ == "__main__":
    main()
