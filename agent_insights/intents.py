"""Heuristic intent classification."""

from .config import INTENT_KEYWORDS
from .db import get_writer


def classify_intent(first_prompt: str, tool_ratios: dict) -> str:
    """Classify session intent from first prompt + tool usage."""
    if not first_prompt:
        return "unknown"

    prompt_lower = first_prompt.lower()

    # Score each intent category
    scores = {}
    for intent, keywords in INTENT_KEYWORDS.items():
        score = 0.0
        for kw in keywords:
            if kw in prompt_lower:
                score += 2.0  # First prompt weighted 2x
        scores[intent] = score

    # Adjust by tool ratios
    edit_ratio = tool_ratios.get("edit_write_ratio", 0)
    read_ratio = tool_ratios.get("read_grep_ratio", 0)
    bash_ratio = tool_ratios.get("bash_ratio", 0)

    scores["implement"] += edit_ratio * 3
    scores["debug"] += bash_ratio * 2
    scores["research"] += read_ratio * 2
    scores["refactor"] += edit_ratio * 2
    scores["review"] += read_ratio * 1.5

    # Pick highest
    if not scores or max(scores.values()) == 0:
        return "unknown"

    best = max(scores, key=scores.get)
    return best


def classify_all_intents():
    """Classify intents for all sessions."""
    conn = get_writer()

    try:
        rows = conn.execute("""
            SELECT s.session_id, s.first_prompt,
                   f.edit_write_ratio, f.read_grep_ratio, f.bash_ratio
            FROM sessions s
            LEFT JOIN session_features f ON s.session_id = f.session_id
        """).fetchall()

        for session_id, first_prompt, edit_r, read_r, bash_r in rows:
            tool_ratios = {
                "edit_write_ratio": edit_r or 0,
                "read_grep_ratio": read_r or 0,
                "bash_ratio": bash_r or 0,
            }
            intent = classify_intent(first_prompt or "", tool_ratios)
            conn.execute(
                "UPDATE sessions SET intent = ? WHERE session_id = ?",
                [intent, session_id],
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(rows)
