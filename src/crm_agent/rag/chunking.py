import re
from dataclasses import dataclass

@dataclass
class Chunk:
    chunk_id: str
    text: str
    meta: dict

def simple_chunk(text: str, meta: dict, base_id: str, max_chars: int = 1200, overlap: int = 150) -> list[Chunk]:
    sentences = re.split(r"(?<=[.!?\n])\s+", text.strip())
    chunks = []
    buf = ""

    for s in sentences:
        if not s:
            continue
        if len(buf) + len(s) + 1 <= max_chars:
            buf = (buf + " " + s).strip()
        else:
            if buf:
                chunks.append(buf)
            buf = s

    if buf:
        chunks.append(buf)

    out = []
    for i, c in enumerate(chunks):
        if overlap > 0 and i > 0:
            prev_tail = chunks[i - 1][-overlap:]
            c = (prev_tail + " " + c).strip()

        out.append(Chunk(
            chunk_id=f"{base_id}__{i:04d}",
            text=c,
            meta={**meta, "chunk_index": i},
        ))
    return out
