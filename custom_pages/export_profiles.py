def render():
    import streamlit as st

    from src.archiefassistent.ui.layout import render_header
    from src.archiefassistent.db import create_export_profile, list_export_profiles
    from src.archiefassistent.shacl import parse_shacl_manifest

    render_header()
    st.header("Export profiles (SHACL)")

    st.subheader("Create SHACL profile")
    name = st.text_input("Profile name", value="New SHACL profile")
    uploaded = st.file_uploader("Upload SHACL file", type=["ttl", "rdf", "xml", "jsonld", "nt"])

    fmt_hint = st.selectbox(
        "Optional format hint",
        options=["(auto)", "turtle", "xml", "json-ld", "nt"],
        index=0
    )

    if uploaded:
        shacl_text = uploaded.getvalue().decode("utf-8", errors="replace")
        if st.button("Parse + Save profile"):
            try:
                fmt = None if fmt_hint == "(auto)" else fmt_hint
                manifest = parse_shacl_manifest(shacl_text, fmt=fmt)
                if not manifest:
                    st.warning("Parsed SHACL but found no sh:NodeShape/sh:property fields.")
                pid = create_export_profile(name=name.strip() or "SHACL profile", kind="shacl", shacl_text=shacl_text, manifest=manifest)
                st.success(f"Saved export profile id={pid} with {len(manifest)} target field(s).")
                with st.expander("Extracted target fields"):
                    st.json(manifest)
            except Exception as e:
                st.error(f"Failed to parse/save SHACL: {e}")

    st.divider()
    st.subheader("Existing profiles")
    profiles = list_export_profiles(kind="shacl")
    if not profiles:
        st.info("No export profiles yet.")
        return

    for p in profiles:
        st.markdown(f"**#{p['id']} — {p['name']}**")
        st.caption(f"Created: {p.get('created_at')} • Fields: {len(p.get('manifest') or [])}")
        with st.expander("Show target fields"):
            st.json(p.get("manifest") or [])
