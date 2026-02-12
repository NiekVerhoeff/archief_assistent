def render():
    import pandas as pd
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.db import list_jobs

    render_header()

    st.header("Welcome — Archiefassistent")
    st.markdown(
        """
    Gebruik de pagina’s in de sidebar:
    - **Process files**: upload bestanden, maak extractie schema en queue een job.
    - **Validate Results**: bekijk resultaten, pas aan.
    - **Export schema's**: maak export schema's.
    - **Export mapping**: koppel geëxtraheerde velden aan export schema.
    - **Run export**: voer export uit naar gewenste formaat/locatie.
    """
    )

    jobs = list_jobs()
    if not jobs:
        st.info("No jobs yet.")
    else:
        df = pd.DataFrame(jobs)
        df["progress"] = df.apply(lambda r: f"{r.get('files_done', 0)}/{r.get('total_files', 0)}", axis=1)
        st.dataframe(df[["id", "name", "root_dir", "model_tag", "created_at", "status", "progress"]], use_container_width=True)
