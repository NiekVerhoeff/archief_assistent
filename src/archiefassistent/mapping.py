from __future__ import annotations
from typing import Dict, Any, Optional, Union
from .schemas import ArchiveMetadata

class MappingTarget:
    DC_JSON = "DublinCoreJSON"
    EAD3_XML = "EAD3_XML"

def to_dublin_core_json(rec: ArchiveMetadata) -> Dict[str, Any]:
    return {
        "title": rec.title,
        "creator": rec.creator,
        "description": rec.description,
        "subject": rec.subjects,
        "date": {"start": rec.date_start, "end": rec.date_end},
        "type": rec.filetype,
        "language": rec.language,
        "rights": rec.rights,
        "coverage": None,
        "relation": None,
        "format": rec.technical.extension,
        "identifier": rec.technical.sha256,
        "sourcePath": rec.technical.path,
        "retention": rec.retention,
        "sensitivity": rec.sensitivity,
    }

def xml_escape(s: Optional[str]) -> str:
    if not s:
        return ""
    return (s.replace("&", "&amp;")
              .replace("<", "&lt;")
              .replace(">", "&gt;")
              .replace('"', "&quot;")
              .replace("'", "&apos;"))

def to_ead3_xml(rec: ArchiveMetadata) -> str:
    subjects_xml = "".join(f"<subject>{xml_escape(s)}</subject>" for s in rec.subjects)
    unitdate_normal = f"{xml_escape(rec.date_start or '')}/{xml_escape(rec.date_end or '')}"
    unitdate_text = (rec.date_start or '') + (f"–{rec.date_end}" if rec.date_end else '')
    return f"""
<ead>
  <archdesc>
    <did>
      <unittitle>{xml_escape(rec.title)}</unittitle>
      <unitdate normal='{unitdate_normal}'>{xml_escape(unitdate_text)}</unitdate>
      <physdesc>{xml_escape(rec.filetype or (rec.technical.extension or ''))}</physdesc>
      <langmaterial>{xml_escape(rec.language)}</langmaterial>
      <dao actuate='onrequest' show='new' href='{xml_escape(rec.technical.path)}'/>
    </did>
    <bioghist>{xml_escape(rec.description)}</bioghist>
    <controlaccess>{subjects_xml}</controlaccess>
    <accessrestrict>{xml_escape(rec.sensitivity or rec.rights)}</accessrestrict>
  </archdesc>
</ead>
""".strip()

def transform_record(rec: ArchiveMetadata, target: str) -> Union[Dict[str, Any], str]:
    if target == MappingTarget.DC_JSON:
        return to_dublin_core_json(rec)
    if target == MappingTarget.EAD3_XML:
        return to_ead3_xml(rec)
    raise ValueError(f"Unknown mapping target: {target}")
