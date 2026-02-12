# src/archiefassistent/ui/preprocess_files.py
def render():
    from datetime import datetime
    from pathlib import Path
    import json
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.config import SUPPORTED_EXTS, UPLOADS_DIR, DEFAULT_MODEL
    from src.archiefassistent.extraction import save_uploaded_files, walk_files, extract_text, sha256_file
    from src.archiefassistent.schemas import FileTechnical, model_to_dict
    from src.archiefassistent.db import create_job, set_job_total_files, update_job_status, increment_job_files_done
    from src.archiefassistent.db import save_preprocess_file, save_preprocess_chunk
    from src.archiefassistent.chunking import chunk_text_with_spans
    from src.archiefassistent.ollama_client import ensure_ollama_ready, list_ollama_models, _ollama_embed

    render_header()
    st.header("Preprocess files (chunk + embed)")

    uploaded = st.file_uploader(
        "Upload files (multiple)",
        type=[e.lstrip(".") for e in SUPPORTED_EXTS],
        accept_multiple_files=True,
    )

    job_name_default = f"preprocess-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    job_name = st.text_input("Preprocess job name", value=job_name_default)

    # You may keep extraction model separate from embedding model
    embed_model = st.text_input("Ollama embedding model", value="qwen3-embedding:0.6b")
    # (Optional) show connectivity
    with st.expander("Diagnostics: Ollama connectivity"):
        st.write("Embedding model available:", "✅" if ensure_ollama_ready(embed_model, timeout_s=4) else "❌")
        st.write("Models:", list_ollama_models(timeout_s=4) or ["(none / not reachable)"])

    st.subheader("Chunking settings")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Summary chunks (large)**")
        sum_chunk_size = st.number_input("Summary chunk size (chars)", min_value=1000, max_value=200000, value=12000, step=500)
        sum_overlap = st.number_input("Summary overlap (chars)", min_value=0, max_value=5000, value=400, step=50)
        sum_max_chunks = st.number_input("Max summary chunks per file", min_value=1, max_value=200, value=10)
    with col2:
        st.markdown("**Embedding/extraction chunks (small)**")
        emb_chunk_size = st.number_input("Embed chunk size (chars)", min_value=200, max_value=20000, value=1200, step=100)
        emb_overlap = st.number_input("Embed overlap (chars)", min_value=0, max_value=2000, value=150, step=25)
        emb_max_chunks = st.number_input("Max embed chunks per file", min_value=1, max_value=500, value=80)

    embed_summary_chunks = st.checkbox("Also embed summary chunks (usually not needed)", value=False)
    timeout_s = st.number_input("Embedding request timeout (s)", min_value=10, max_value=300, value=60)

    if not uploaded:
        st.info("Upload files to start preprocessing.")
        return

    st.info(f"{len(uploaded)} file(s) ready")
    if st.button("Run preprocessing now"):
        # Save uploads to disk (reusing your flow)
        dest = UPLOADS_DIR / job_name
        upload_dir = save_uploaded_files(uploaded, dest)

        # Store options on job so later processing can reuse them
        job_options = {
            "preprocess": {
                "summary": {"chunk_size": int(sum_chunk_size), "overlap": int(sum_overlap), "max_chunks": int(sum_max_chunks)},
                "embed": {"chunk_size": int(emb_chunk_size), "overlap": int(emb_overlap), "max_chunks": int(emb_max_chunks)},
                "embed_model": embed_model,
                "embed_summary_chunks": bool(embed_summary_chunks),
            }
        }

        job_id = create_job(job_name, str(upload_dir), model_tag=DEFAULT_MODEL, options=job_options)
        update_job_status(job_id, "preprocessing")

        files = walk_files(upload_dir)
        set_job_total_files(job_id, len(files))

        prog = st.progress(0)
        done = 0

        for fp in files:
            try:
                text = extract_text(fp)

                # Technical metadata (extend as needed)
                tech = FileTechnical(
                    path=str(fp),
                    filename=fp.name,
                    extension=fp.suffix.lower(),
                    size_bytes=fp.stat().st_size,
                    sha256=sha256_file(fp),
                )

                # Store file metadata
                preprocess_file_id = save_preprocess_file(
                    job_id=job_id,
                    filename=fp.name,
                    path=str(fp),
                    technical=model_to_dict(tech),
                )

                # --- Summary chunks (large) ---
                sum_chunks = chunk_text_with_spans(
                    text,
                    chunk_size=int(sum_chunk_size),
                    overlap=int(sum_overlap),
                    max_chunks=int(sum_max_chunks),
                )
                for idx, (stc, endc, ch) in enumerate(sum_chunks):
                    emb = None
                    if embed_summary_chunks:
                        emb = _ollama_embed(ch, model=embed_model, timeout_s=int(timeout_s))
                    save_preprocess_chunk(
                        job_id=job_id,
                        preprocess_file_id=preprocess_file_id,
                        filename=fp.name,
                        chunk_type="summary",
                        chunk_index=idx,
                        start_char=stc,
                        end_char=endc,
                        chunk_text=ch,
                        embedding=emb,
                    )

                # --- Embedding chunks (small) ---
                emb_chunks = chunk_text_with_spans(
                    text,
                    chunk_size=int(emb_chunk_size),
                    overlap=int(emb_overlap),
                    max_chunks=int(emb_max_chunks),
                )
                for idx, (stc, endc, ch) in enumerate(emb_chunks):
                    vec = _ollama_embed(ch, model=embed_model, timeout_s=int(timeout_s))
                    save_preprocess_chunk(
                        job_id=job_id,
                        preprocess_file_id=preprocess_file_id,
                        filename=fp.name,
                        chunk_type="embed",
                        chunk_index=idx,
                        start_char=stc,
                        end_char=endc,
                        chunk_text=ch,
                        embedding=vec,
                    )

            except Exception as e:
                st.warning(f"Failed preprocessing {fp.name}: {e}")

            done += 1
            increment_job_files_done(job_id)
            prog.progress(min(1.0, done / max(1, len(files))))

        update_job_status(job_id, "preprocessed")
        st.success(f"Preprocessing finished. Job id={job_id}")
        st.caption("Chunks + embeddings stored in preprocess_chunks; technical metadata stored in preprocess_files.")
