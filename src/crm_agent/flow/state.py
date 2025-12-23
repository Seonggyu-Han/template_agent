from typing import TypedDict

class FlowState(TypedDict, total=False):
    run_id: str
    brief: dict
    channel: str
    tone: str
    target: dict
    rag: dict
    candidates: dict
    selected: dict
    final: dict
