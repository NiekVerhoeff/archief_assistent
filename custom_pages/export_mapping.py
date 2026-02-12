def render():
    import json
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.db import (
        list_jobs, get_export_profile, list_export_profiles,
        get_job_export_mapping, upsert_job_export_mapping
    )

    render_header()
    st.header("Export mapping (Job → SHACL)")

    jobs = list_jobs()
    if not jobs:
        st.info("No jobs available.")
        return

    profiles = list_export_profiles(kind="shacl")
    if not profiles:
        st.info("No SHACL export profiles. Create one first.")
        return

    # Select job + profile
    job_ids = [j["id"] for j in jobs]
    job_id = st.selectbox("Job", options=job_ids, format_func=lambda x: f"#{x} — {next(j['name'] for j in jobs if j['id']==x)}")

    prof_ids = [p["id"] for p in profiles]
    profile_id = st.selectbox("SHACL profile", options=prof_ids, format_func=lambda x: f"#{x} — {next(p['name'] for p in profiles if p['id']==x)}")

    # Load job schema fields
    job = next(j for j in jobs if j["id"] == job_id)
    schema = (job.get("options") or {}).get("schema") or {}
    props = (schema.get("properties") or {}) if isinstance(schema, dict) else {}

    source_fields = list(props.keys())
    if not source_fields:
        st.error("Selected job has no schema/properties stored in options_json['schema'].")
        return

    # Load profile manifest
    profile = get_export_profile(int(profile_id))
    if not profile:
        st.error("Export profile not found.")
        return

    manifest = profile.get("manifest") or []
    if not manifest:
        st.error("This profile has no extracted target fields.")
        return

    # Load existing mapping
    existing = get_job_export_mapping(int(job_id), int(profile_id)) or {}

    st.subheader("Map source fields to target fields")
    st.caption("For each SHACL target field, choose a source field from the job schema. Leave blank to skip.")

    mapping: dict = {}
    required_missing = 0

    for f in manifest:
        tgt_key = f.get("key")
        label = f.get("label") or tgt_key
        required = bool(f.get("required"))

        default_source = (existing.get(tgt_key) or {}).get("source") if isinstance(existing.get(tgt_key), dict) else existing.get(tgt_key)

        col1, col2, col3 = st.columns([2, 2, 2])
        with col1:
            st.write(f"**{label}**")
            st.caption(tgt_key)
        with col2:
            src = st.selectbox(
                "Source field",
                options=["(skip)"] + source_fields,
                index=(["(skip)"] + source_fields).index(default_source) if default_source in source_fields else 0,
                key=f"map_{job_id}_{profile_id}_{tgt_key}",
                label_visibility="collapsed",
            )
        with col3:
            # minimal transforms list for now
            transform = st.selectbox(
                "Transform",
                options=["(none)", "join:semicolon", "join:comma", "first", "strip"],
                index=0,
                key=f"tx_{job_id}_{profile_id}_{tgt_key}",
                label_visibility="collapsed",
            )

        if src != "(skip)":
            mapping[tgt_key] = {"source": src, "transform": None if transform == "(none)" else transform}
        elif required:
            required_missing += 1

    if required_missing:
        st.warning(f"{required_missing} required SHACL fields are not mapped.")

    if st.button("Save mapping"):
        upsert_job_export_mapping(int(job_id), int(profile_id), mapping)
        st.success("Mapping saved.")
        with st.expander("Saved mapping JSON"):
            st.json(mapping)
