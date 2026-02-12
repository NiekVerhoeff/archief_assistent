def render():
    from pathlib import Path
    import json

    import pandas as pd
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.db import (
        list_jobs,
        get_job_records,
        update_record_db,
        delete_job,
        get_job,
    )

    render_header()
    st.header("Jobs / Results")

    # ----------------------------
    # List jobs
    # ----------------------------
    jobs = list_jobs()
    if not jobs:
        st.info("No jobs yet.")
        st.stop()

    dfj = pd.DataFrame(jobs)
    dfj["progress"] = dfj.apply(lambda r: f"{r.get('files_done', 0)}/{r.get('total_files', 0)}", axis=1)
    st.dataframe(
        dfj[["id", "name", "root_dir", "model_tag", "created_at", "status", "progress"]],
        use_container_width=True,
    )

    min_id = int(dfj["id"].min())
    max_id = int(dfj["id"].max())

    # ----------------------------
    # Session state: keep job open across reruns
    # ----------------------------
    if "loaded_job_id" not in st.session_state:
        st.session_state.loaded_job_id = None

    # Default selection in input: loaded job if any, otherwise latest
    default_sel = int(st.session_state.loaded_job_id) if st.session_state.loaded_job_id is not None else max_id
    default_sel = max(min_id, min(max_id, default_sel))

    sel = st.number_input(
        "Open job id",
        min_value=min_id,
        max_value=max_id,
        value=default_sel,
    )

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Load job", key="load_job"):
            st.session_state.loaded_job_id = int(sel)

    with colB:
        delbtn = st.button("Delete job", key="delete_job")

    if delbtn:
        delete_job(int(sel))
        if st.session_state.loaded_job_id == int(sel):
            st.session_state.loaded_job_id = None
        st.success(f"Job {sel} deleted.")
        st.rerun()

    # Gate: don't stop based on the transient button state
    if st.session_state.loaded_job_id is None:
        st.info("Load a job to view and edit its records.")
        st.stop()

    job_id = int(st.session_state.loaded_job_id)

    # ----------------------------
    # Helpers (schema-driven table)
    # ----------------------------
    def _schema_property_keys(schema: dict) -> list[str]:
        props = (schema or {}).get("properties") or {}
        return list(props.keys()) if isinstance(props, dict) else []

    def _schema_default_row(schema: dict) -> dict:
        out = {}
        props = (schema or {}).get("properties") or {}
        if not isinstance(props, dict):
            return out

        for k, s in props.items():
            if not isinstance(s, dict):
                out[k] = None
                continue

            t = s.get("type")
            if isinstance(t, list):
                non_null = [x for x in t if x != "null"]
                t0 = non_null[0] if non_null else "string"
            else:
                t0 = t

            if t0 == "array":
                out[k] = "[]"
            elif t0 == "object":
                out[k] = "{}"
            else:
                out[k] = None
        return out

    def _record_to_row(record: dict, schema: dict) -> dict:
        row = _schema_default_row(schema)
        props = (schema or {}).get("properties") or {}

        for k in row.keys():
            v = record.get(k)

            s = props.get(k) if isinstance(props, dict) else None
            t = s.get("type") if isinstance(s, dict) else None
            if isinstance(t, list):
                non_null = [x for x in t if x != "null"]
                t0 = non_null[0] if non_null else None
            else:
                t0 = t

            if t0 == "array" or isinstance(v, list):
                try:
                    row[k] = json.dumps(v if v is not None else [], ensure_ascii=False)
                except Exception:
                    row[k] = "[]"
            elif t0 == "object" or isinstance(v, dict):
                try:
                    row[k] = json.dumps(v if v is not None else {}, ensure_ascii=False)
                except Exception:
                    row[k] = "{}"
            else:
                row[k] = v

        return row

    def _row_to_record(row: dict, schema: dict) -> dict:
        out = {}
        props = (schema or {}).get("properties") or {}

        for k, v in row.items():
            if k.startswith("__"):
                continue

            s = props.get(k) if isinstance(props, dict) else None
            t = s.get("type") if isinstance(s, dict) else None
            if isinstance(t, list):
                non_null = [x for x in t if x != "null"]
                t0 = non_null[0] if non_null else None
            else:
                t0 = t

            if t0 == "array":
                if v is None or v == "":
                    out[k] = []
                elif isinstance(v, list):
                    out[k] = v
                elif isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        out[k] = parsed if isinstance(parsed, list) else [str(parsed)]
                    except Exception:
                        out[k] = [v] if v else []
                else:
                    out[k] = []
            elif t0 == "object":
                if v is None or v == "":
                    out[k] = {}
                elif isinstance(v, dict):
                    out[k] = v
                elif isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        out[k] = parsed if isinstance(parsed, dict) else {}
                    except Exception:
                        out[k] = {}
                else:
                    out[k] = {}
            else:
                out[k] = v

        return out

    # ----------------------------
    # Load job + schema
    # ----------------------------
    job = get_job(job_id)
    if not job:
        st.error("Job not found.")
        st.session_state.loaded_job_id = None
        st.stop()

    schema = (job.get("options") or {}).get("schema") or {}
    if not schema:
        st.warning("This job has no custom schema stored; using DEFAULT_SCHEMA fields.")
        from src.archiefassistent.ollama_client import DEFAULT_SCHEMA

        schema = DEFAULT_SCHEMA

    schema_keys = _schema_property_keys(schema)
    if not schema_keys:
        st.error("Schema has no properties; cannot build results table.")
        st.stop()

    # ----------------------------
    # Load records for this job
    # ----------------------------
    recs = get_job_records(job_id)
    if not recs:
        st.info("No records for this job.")
        st.stop()

    rows = []
    for r in recs:
        row = _record_to_row(r, schema)
        row["__db_id"] = r.get("__db_id")
        row["__filename"] = r.get("__filename")
        row["__created_at"] = r.get("__created_at")
        rows.append(row)

    df = pd.DataFrame(rows)

    # Consistent column order
    ordered_cols = schema_keys + ["__filename", "__created_at", "__db_id"]
    ordered_cols = [c for c in ordered_cols if c in df.columns]
    df = df[ordered_cols]

    # Column config: arrays/objects as JSON text
    props = schema.get("properties") or {}
    column_config = {}
    for k in schema_keys:
        s = props.get(k) if isinstance(props, dict) else None
        t = s.get("type") if isinstance(s, dict) else None
        if isinstance(t, list):
            non_null = [x for x in t if x != "null"]
            t0 = non_null[0] if non_null else None
        else:
            t0 = t

        if t0 in ("array", "object"):
            column_config[k] = st.column_config.TextColumn(f"{k} (JSON)")

    # Keep internal columns visible but non-editable
    column_config["__db_id"] = st.column_config.NumberColumn("__db_id", disabled=True)
    column_config["__filename"] = st.column_config.TextColumn("__filename", disabled=True)
    column_config["__created_at"] = st.column_config.TextColumn("__created_at", disabled=True)

    # IMPORTANT: key must be stable while editing; use loaded job id
    edited = st.data_editor(
        df,
        use_container_width=True,
        key=f"editor_{job_id}",
        num_rows="dynamic",
        column_config=column_config,
    )

    # ----------------------------
    # Save
    # ----------------------------
    if st.button("Save edits for job", key=f"save_{job_id}"):
        edited2 = edited.copy()

        # Safety: ensure ids exist
        if "__db_id" not in edited2.columns:
            edited2["__db_id"] = df["__db_id"].values

        # Convert each row to record_json and save
        for _, row in edited2.iterrows():
            rid_val = row.get("__db_id")
            if rid_val is None:
                continue
            try:
                rid = int(rid_val)
            except Exception:
                continue

            record_json = _row_to_record(dict(row), schema)
            update_record_db(rid, record_json)

        st.success("Saved edits.")
        st.rerun()

    st.caption("Clean V2 — modular refactor; jobs queue + worker + edit preserved.")
