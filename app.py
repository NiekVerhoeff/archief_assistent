import streamlit as st
from src.archiefassistent.config import APP_TITLE, PAGE_ICON
from src.archiefassistent.ui.layout import render_sidebar
from custom_pages import home, preprocess_files, process_files, results_export, export_mapping, export_profiles, export_run

st.set_page_config(page_title=APP_TITLE, page_icon=PAGE_ICON, layout="wide")

render_sidebar()

page = st.session_state.get("page", "home")

if page == "home":
    home.render()
elif page == "preprocess":
    preprocess_files.render()
elif page == "process":
    process_files.render()
elif page == "results":
    results_export.render()
elif page == "export_mapping":
    export_mapping.render()
elif page == "export_profiles":
    export_profiles.render()
elif page == "export_run":
    export_run.render()
else:
    st.error("Unknown page")


