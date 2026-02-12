from __future__ import annotations
from typing import Any, Dict, List, Optional

def parse_shacl_manifest(shacl_text: str, *, fmt: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Parse SHACL shapes and extract a normalized list of target fields.

    Output format:
      [
        {"key": "<path>", "label": "<human>", "datatype": "<datatype/class>", "required": bool}
      ]

    Notes:
    - Prefers Dutch labels (@nl), then English (@en), then any.
    - Reads label from sh:name or rdfs:label.
    - Supports complex sh:path values (blank nodes / lists) by using N3 serialization.
    """
    try:
        from rdflib import Graph, Namespace, RDF, RDFS, URIRef, Literal
        from rdflib.term import BNode
    except Exception as e:
        raise RuntimeError("rdflib is required for SHACL parsing. Install with: pip install rdflib") from e

    SH = Namespace("http://www.w3.org/ns/shacl#")

    g = Graph()

    # Your example is Turtle; default to turtle for robustness
    parse_format = fmt or "turtle"
    g.parse(data=shacl_text, format=parse_format)

    # --- helpers ---
    def _pick_lang_literal(lits: List[Literal], prefer: List[str] = ["nl", "en"]) -> Optional[str]:
        """Pick best literal by language preference."""
        if not lits:
            return None
        # exact language match first
        for lang in prefer:
            for l in lits:
                if getattr(l, "language", None) == lang:
                    return str(l)
        # then no language
        for l in lits:
            if getattr(l, "language", None) in (None, ""):
                return str(l)
        # else first
        return str(lits[0])

    def _get_best_label(node) -> Optional[str]:
        # Try sh:name, then rdfs:label
        name_lits = [o for o in g.objects(node, SH.name) if isinstance(o, Literal)]
        if name_lits:
            return _pick_lang_literal(name_lits)

        label_lits = [o for o in g.objects(node, RDFS.label) if isinstance(o, Literal)]
        if label_lits:
            return _pick_lang_literal(label_lits)

        return None

    def _qname_or_uri(u: URIRef) -> str:
        try:
            return g.qname(u)
        except Exception:
            return str(u)

    def _path_key(path_node) -> str:
        """
        Return a stable string key for sh:path.
        - URIRef => qname if possible
        - otherwise => N3 representation (handles blank nodes / complex paths)
        """
        if isinstance(path_node, URIRef):
            return _qname_or_uri(path_node)
        try:
            # n3() gives something like _:b0 or a bracketed expression for some nodes
            return path_node.n3(g.namespace_manager)  # type: ignore
        except Exception:
            return str(path_node)

    def _datatype_or_class(ps) -> str:
        dt = next(iter(g.objects(ps, SH.datatype)), None)
        if isinstance(dt, URIRef):
            return _qname_or_uri(dt)

        cls = next(iter(g.objects(ps, SH["class"])), None)
        if isinstance(cls, URIRef):
            return _qname_or_uri(cls)

        nk = next(iter(g.objects(ps, SH.nodeKind)), None)
        if isinstance(nk, URIRef):
            # if only nodeKind is known, expose it (e.g., sh:IRI)
            return _qname_or_uri(nk)

        return ""

    def _required(ps) -> bool:
        mc = next(iter(g.objects(ps, SH.minCount)), None)
        if mc is None:
            return False
        try:
            return int(str(mc)) >= 1
        except Exception:
            return False

    # --- main extraction ---
    manifest: List[Dict[str, Any]] = []

    node_shapes = set(g.subjects(RDF.type, SH.NodeShape))
    for ns in node_shapes:
        for ps in g.objects(ns, SH.property):
            path = next(iter(g.objects(ps, SH.path)), None)
            if path is None:
                continue

            key = _path_key(path)

            # label preference: property shape label/name first; fallback to path
            label = _get_best_label(ps) or _get_best_label(path) or key

            manifest.append(
                {
                    "key": key,
                    "label": label,
                    "datatype": _datatype_or_class(ps),
                    "required": _required(ps),
                }
            )

    # Deduplicate by key (keep first occurrence)
    seen = set()
    out: List[Dict[str, Any]] = []
    for f in manifest:
        k = f.get("key")
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(f)

    # Sort: required first, then label
    out.sort(key=lambda x: (not bool(x.get("required")), str(x.get("label") or "")))
    return out
