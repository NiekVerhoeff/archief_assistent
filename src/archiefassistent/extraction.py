from __future__ import annotations
import hashlib
import zipfile
from pathlib import Path
from typing import List, Any

from bs4 import BeautifulSoup
from chardet.universaldetector import UniversalDetector

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    import docx
except Exception:
    docx = None

from .config import SUPPORTED_EXTS


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_text_with_encoding(path: Path) -> str:
    detector = UniversalDetector()
    with open(path, "rb") as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    enc = detector.result.get("encoding") or "utf-8"
    try:
        return path.read_text(encoding=enc, errors="replace")
    except Exception:
        return path.read_text(encoding="utf-8", errors="replace")


def extract_text(path: Path) -> str:
    ext = path.suffix.lower()

    if ext in {".txt", ".md"}:
        return read_text_with_encoding(path)

    if ext in {".html", ".htm"}:
        html = read_text_with_encoding(path)
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n")

    if ext == ".pdf":
        if PdfReader is None:
            return ""
        try:
            reader = PdfReader(str(path))
            pages = []
            for p in reader.pages:
                try:
                    pages.append(p.extract_text() or "")
                except Exception:
                    pages.append("")
            return "\n".join(pages)
        except Exception:
            return ""

    if ext == ".docx":
        if docx is None:
            return ""
        try:
            d = docx.Document(str(path))
            return "\n".join(p.text for p in d.paragraphs)
        except Exception:
            return ""

    return ""


def walk_files(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS]


def save_uploaded_files(uploaded_files: List[Any], dest: Path) -> Path:
    """Write UploadedFile objects to dest.
    If a ZIP is uploaded it will be extracted into dest/<zip-stem>/ preserving structure.
    """
    dest.mkdir(parents=True, exist_ok=True)

    for f in uploaded_files:
        fname = Path(f.name).name
        out = dest / fname

        with open(out, "wb") as fh:
            fh.write(f.getbuffer())

        if out.suffix.lower() == ".zip":
            try:
                extract_base = dest / out.stem
                extract_base.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(out, "r") as z:
                    z.extractall(path=str(extract_base))
                out.unlink(missing_ok=True)
            except Exception:
                pass

    return dest
