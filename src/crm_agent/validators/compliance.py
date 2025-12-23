BANNED = ["무조건", "100% 효과", "완치", "영구", "절대", "확실히", "단번에"]
MEDICAL = ["치료", "처방", "진단", "병명"]

def validate_text(text: str) -> tuple[str, list[str]]:
    """
    MVP 컴플라이언스:
    - 의료/치료 암시는 FAIL
    - 과장/확정형 표현은 WARN
    """
    t = (text or "").lower()
    issues: list[str] = []

    for b in BANNED:
        if b.lower() in t:
            issues.append(f"WARN: 과장/금칙 가능 표현 '{b}'")

    for m in MEDICAL:
        if m.lower() in t:
            issues.append(f"FAIL: 의료/치료 암시 '{m}'")

    status = "PASS"
    if any(i.startswith("FAIL") for i in issues):
        status = "FAIL"
    elif issues:
        status = "WARN"

    return status, issues
