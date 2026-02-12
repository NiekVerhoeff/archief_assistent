import streamlit as st
from ..config import APP_TITLE, LOGO_PATH

def render_sidebar():
    """Render the global sidebar with logo + navigation."""
    # Logo (optional)
    if LOGO_PATH.exists():
        st.sidebar.image(str(LOGO_PATH), width=120)

    if "page" not in st.session_state:
        st.session_state.page = "home"
    
    sb = st.sidebar
    sb.markdown("### Navigatie")

    if sb.button("Home", icon=":material/home:"):
        st.session_state.page = "home"

    if sb.button("Preprocess files", icon=":material/upload_file:"):
        st.session_state.page = "preprocess"    

    if sb.button("Process files", icon=":material/document_scanner:"):
        st.session_state.page = "process"

    if sb.button("Validate Results", icon=":material/rule:"):
        st.session_state.page = "results"
        
    if sb.button("Export profiles", icon=":material/graph_3:"):
        st.session_state.page = "export_profiles"

    if sb.button("Export mapping", icon=":material/flowchart:"):
        st.session_state.page = "export_mapping"
    
    if sb.button("Run export", icon=":material/file_export:"):
        st.session_state.page = "export_run"

def render_header():
    if LOGO_PATH.exists():
        
        col1, col2 = st.columns([0.9, 8])
        with col1:
            st.image(str(LOGO_PATH), width=96)
        with col2:
            st.markdown(f"# {APP_TITLE}")

    else:
        st.title(APP_TITLE)

