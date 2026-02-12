def render():
    from pathlib import Path
    import json
    import io
    import zipfile
    import re
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.db import (
        list_jobs,
        list_export_profiles,
        get_job_records,
        get_job_export_mapping,
    )
    from src.archiefassistent.export_runner import map_record

    render_header()
    st.header("Run export (SHACL mapping → JSON)")

    jobs = list_jobs()
    profiles = list_export_profiles(kind="shacl")
    if not jobs or not profiles:
        st.info("Need at least one job and one SHACL profile.")
        return

    jobs_by_id = {int(j["id"]): j for j in jobs}
    profiles_by_id = {int(p["id"]): p for p in profiles}

    job_id = st.selectbox(
        "Job",
        [j["id"] for j in jobs],
        format_func=lambda x: f"#{x} — {jobs_by_id[int(x)]['name']}",
    )
    profile_id = st.selectbox(
        "SHACL profile",
        [p["id"] for p in profiles],
        format_func=lambda x: f"#{x} — {profiles_by_id[int(x)]['name']}",
    )

    mapping = get_job_export_mapping(int(job_id), int(profile_id))
    if not mapping:
        st.warning("No mapping saved for this job/profile. Go to Export mapping first.")
        return

    recs = get_job_records(int(job_id))
    if not recs:
        st.info("No records for this job.")
        return

    n = st.number_input("Export first N records", min_value=1, max_value=len(recs), value=len(recs))

    save_to_disk = st.checkbox("Also save files to disk", value=True)
    outdir = None
    if save_to_disk:
        outdir = Path(
            st.text_input("Export directory", value=str(Path.cwd() / "archiefassistent_exports"))
        )
        outdir.mkdir(parents=True, exist_ok=True)
    else:
        st.caption("Files will not be written to disk — download will still work.")

    # keep exports available after rerun
    if "export_outputs" not in st.session_state:
        st.session_state.export_outputs = None  # list of dicts with filename + bytes + optional path

    def _safe_slug(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
        return s.strip("_") or "export"

    if st.button("Export"):
        outputs = []  # list of {filename, bytes, path?}

        job_name = _safe_slug(jobs_by_id[int(job_id)].get("name") or f"job_{job_id}")
        profile_name = _safe_slug(profiles_by_id[int(profile_id)].get("name") or f"profile_{profile_id}")

        for i, r in enumerate(recs[: int(n)]):
            # remove internal keys from db record
            r2 = dict(r)
            r2.pop("__db_id", None)
            r2.pop("__filename", None)
            r2.pop("__created_at", None)

            mapped = map_record(r2, mapping)

            src_name = (r.get("__filename") or f"record_{i}").replace("/", "_")
            base = _safe_slug(Path(src_name).stem) or f"record_{i}"
            filename = f"{base}.mapped.json"

            json_text = json.dumps(mapped, ensure_ascii=False, indent=2)
            data_bytes = json_text.encode("utf-8")

            path_str = None
            if save_to_disk and outdir is not None:
                outfile = outdir / filename
                outfile.write_bytes(data_bytes)
                path_str = str(outfile)

            outputs.append(
                {
                    "filename": filename,
                    "bytes": data_bytes,
                    "path": path_str,
                }
            )

        st.session_state.export_outputs = {
            "job_id": int(job_id),
            "profile_id": int(profile_id),
            "job_name": job_name,
            "profile_name": profile_name,
            "outputs": outputs,
            "saved_to_disk": bool(save_to_disk),
            "outdir": str(outdir) if outdir is not None else None,
        }

        if save_to_disk and outdir is not None:
            st.success(f"Exported {len(outputs)} file(s) to {outdir}")
            with st.expander("Paths"):
                st.write("\n".join([o["path"] for o in outputs if o.get("path")]))
        else:
            st.success(f"Exported {len(outputs)} file(s). Use download below.")

    # ---- Download section ----
    payload = st.session_state.export_outputs
    if payload and payload.get("outputs"):
        st.markdown("### Download")

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for o in payload["outputs"]:
                zf.writestr(o["filename"], o["bytes"])
        zip_buf.seek(0)

        zip_name = f"{payload['job_name']}__{payload['profile_name']}__mapped.zip"
        st.download_button(
            label="⬇️ Download all as ZIP",
            data=zip_buf.getvalue(),
            file_name=zip_name,
            mime="application/zip",
            use_container_width=True,
        )

        with st.expander("Download individual files"):
            for o in payload["outputs"]:
                st.download_button(
                    label=f"Download {o['filename']}",
                    data=o["bytes"],
                    file_name=o["filename"],
                    mime="application/json",
                    key=f"dl_{payload['job_id']}_{payload['profile_id']}_{o['filename']}",
                )
