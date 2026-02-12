from pathlib import Path

APP_TITLE = "De Archiefassistent"
DEFAULT_MODEL = "llama3.2:3b"
OLLAMA_BASE = "http://localhost:11434"

SUPPORTED_EXTS = {".txt", ".md", ".pdf", ".docx", ".html", ".htm", ".zip"}

CACHE_DIR = Path.home() / ".archiefassistent_cache"
DB_PATH = CACHE_DIR / "jobs.db"
UPLOADS_DIR = CACHE_DIR / "uploads"

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # archiefassistent/
ASSETS_DIR = PROJECT_ROOT / "assets"
LOGO_PATH = ASSETS_DIR / "AA-logo.png"

PAGE_ICON = str(LOGO_PATH) if LOGO_PATH.exists() else "📚"
