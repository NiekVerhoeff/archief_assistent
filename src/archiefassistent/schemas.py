from __future__ import annotations
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

try:
    from pydantic import ConfigDict  # pydantic v2
    _HAS_PYDANTIC_V2 = True
except Exception:
    ConfigDict = dict  # type: ignore
    _HAS_PYDANTIC_V2 = False


class FileTechnical(BaseModel):
    path: str
    filename: str
    extension: Optional[str]
    size_bytes: int
    sha256: str


class ArchiveMetadata(BaseModel):
    title: Optional[str] = Field(None)
    description: Optional[str] = None
    creator: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    subjects: List[str] = Field(default_factory=list)
    language: Optional[str] = None
    rights: Optional[str] = None
    sensitivity: Optional[str] = None
    retention: Optional[str] = None
    filetype: Optional[str] = None
    technical: FileTechnical

    if _HAS_PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config:
            extra = "ignore"


def model_to_dict(m: BaseModel) -> Dict[str, Any]:
    if m is None:
        return {}
    if hasattr(m, "model_dump"):
        return m.model_dump()  # type: ignore[attr-defined]
    return m.dict()
