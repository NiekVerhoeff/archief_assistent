from typing import List, Tuple

def chunk_text(text: str, chunk_size: int = 2200, overlap: int = 150, max_chunks: int = 5) -> List[str]:
    if not text:
        return []
    chunk_size = max(200, int(chunk_size))
    overlap = max(0, int(overlap))

    chunks: List[str] = []
    start = 0
    n = len(text)

    while start < n and len(chunks) < max(1, int(max_chunks)):
        end = min(start + chunk_size, n)
        chunks.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)

    return chunks

def chunk_text_with_spans(
    text: str,
    chunk_size: int = 2200,
    overlap: int = 150,
    max_chunks: int = 5
) -> List[Tuple[int, int, str]]:
    if not text:
        return []
    chunk_size = max(200, int(chunk_size))
    overlap = max(0, int(overlap))

    out: List[Tuple[int, int, str]] = []
    start = 0
    n = len(text)

    while start < n and len(out) < max(1, int(max_chunks)):
        end = min(start + chunk_size, n)
        out.append((start, end, text[start:end]))
        if end == n:
            break
        start = max(0, end - overlap)

    return out