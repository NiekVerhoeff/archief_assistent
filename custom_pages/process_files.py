def render():
    from datetime import datetime
    from pathlib import Path
    import json

    import streamlit as st
    import streamlit.components.v1 as components
    from streamlit_ace import st_ace
    from streamlit_monaco import st_monaco

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.config import DEFAULT_MODEL, SUPPORTED_EXTS, UPLOADS_DIR
    from src.archiefassistent.extraction import save_uploaded_files, walk_files
    from src.archiefassistent.db import create_job, set_job_total_files
    from src.archiefassistent.ollama_client import ensure_ollama_ready, list_ollama_models, generate_json_schema, DEFAULT_SCHEMA

    render_header()

    st.header("Start processing job")
    st.subheader("Upload files from your machine")

    uploaded = st.file_uploader(
        "Upload files to create a processing job (multiple)",
        type=[e.lstrip(".") for e in SUPPORTED_EXTS],
        accept_multiple_files=True,
    )

    model_tag = st.text_input("Ollama model", value=DEFAULT_MODEL)
    job_name_default = f"job-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    job_name = st.text_input("Job name", value=job_name_default)

    # (kept) UI knobs: currently not applied to worker to keep behavior identical,
    # but you can wire them into jobs.process_job later.
    max_files = st.number_input("Max files to process", min_value=1, max_value=5000, value=200)
    request_timeout = st.number_input("Model timeout (s)", min_value=30, max_value=600, value=180)
    chunk_size = st.number_input("Chunk size (chars)", min_value=400, max_value=120000, value=2200, step=200)
    chunk_overlap = st.number_input("Chunk overlap (chars)", min_value=0, max_value=1000, value=150, step=25)
    max_chunks = st.number_input("Max chunks per file", min_value=1, max_value=20, value=5)

    job_options = {
        "max_files": int(max_files),
        "request_timeout": int(request_timeout),
        "chunk_size": int(chunk_size),
        "chunk_overlap": int(chunk_overlap),
        "max_chunks": int(max_chunks),
    }

    with st.expander("Diagnostics: Ollama connectivity"):
        ok = ensure_ollama_ready(model_tag, timeout_s=4)
        st.write("Model available:" , "✅" if ok else "❌")
        st.write("Models:", list_ollama_models(timeout_s=4) or ["(none / not reachable)"])
    
    # --- Schema builder UI ---
    st.subheader("Extractie schema")
    schema_desc = st.text_area(
        "Beschrijf de velden die je wilt extraheren om een extractie schema te genereren",
        value="",
        placeholder="Voorbeeld: Beschrijf titel, samenvatting, maker/organisatie, datum (start/eind), locatie, onderwerpen, gevoeligheid (openbaar/intern/vertrouwelijk), bewaartermijn, rechten, taal.",
        height=120,
    )

    # Keep schema in session state
    if "schema_text" not in st.session_state:
        st.session_state.schema_text = json.dumps(DEFAULT_SCHEMA, ensure_ascii=False, indent=2)

    colA, colB = st.columns([1, 2])

    with colA:
        generate_schema = st.button("Generate schema with LLM", disabled=not schema_desc.strip())

    with colB:
        st.caption("Je kunt het schema hieronder aanpassen. Het moet wel valide JSON zijn.")

    # When user clicks generate
    if generate_schema:
        # call your LLM to generate a JSON Schema
        # We'll define `generate_json_schema(...)` below
        try:
            generated = generate_json_schema(model=model_tag, description=schema_desc)
            st.session_state.schema_text = json.dumps(generated, ensure_ascii=False, indent=2)
            st.success("Schema gegenereerd. Bekijk en bewerk het hieronder.")
        except Exception as e:
            st.error(f"Failed to generate schema: {e}")
    
    schema_text = st_ace(
        value=st.session_state.schema_text,
        language="json",
        theme="chrome",   # or "monokai", "github"
        height=320,
        key="schema_editor"
    )

    # Validate schema JSON live
    schema_obj = None
    schema_error = None
    try:
        schema_obj = json.loads(schema_text)
        if not isinstance(schema_obj, dict):
            schema_error = "Schema must be a JSON object."
    except Exception as e:
        schema_error = str(e)

    if schema_error:
        st.warning(f"Schema JSON invalid: {schema_error}")
    else:
        # basic safety defaults: object schema, no extra properties unless user wants them
        st.success("Schema JSON is valid.")

    if schema_obj:
        job_options["schema"] = schema_obj

    can_queue = (schema_error is None)

    if uploaded:
        st.info(f"{len(uploaded)} file(s) ready to queue")
        for f in uploaded:
            st.write(f"- {f.name} ({f.size} bytes)")

        if st.button("Create queued job from uploads", disabled=not can_queue):
            dest = UPLOADS_DIR / job_name
            upload_dir = save_uploaded_files(uploaded, dest)

            job_id = create_job(job_name, str(upload_dir), model_tag, options=job_options)
            files = walk_files(upload_dir)
            set_job_total_files(job_id, len(files))

            st.success(f"Uploaded files saved to {upload_dir}. Job queued (id={job_id}).")

   