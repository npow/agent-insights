"""Per-session feature extraction."""

import math
import re
from .config import (
    CORRECTION_MARKERS,
    REPHRASING_MARKERS,
    DECISION_MARKERS,
    TOOL_CATEGORIES,
)
from .db import get_writer


def _linear_trend(values: list[float]) -> float:
    """Simple linear regression slope, normalized by mean."""
    n = len(values)
    if n < 2:
        return 0.0
    mean_y = sum(values) / n
    if mean_y == 0:
        return 0.0
    mean_x = (n - 1) / 2.0
    num = sum((i - mean_x) * (v - mean_y) for i, v in enumerate(values))
    den = sum((i - mean_x) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    slope = num / den
    return slope / mean_y


def _coefficient_of_variation(values: list[float]) -> float:
    """CV = std/mean."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance) / mean


def _oscillation_score(values: list[float]) -> float:
    """Measure direction changes (up/down oscillation)."""
    if len(values) < 3:
        return 0.0
    changes = 0
    for i in range(2, len(values)):
        d1 = values[i - 1] - values[i - 2]
        d2 = values[i] - values[i - 1]
        if d1 * d2 < 0:
            changes += 1
    return changes / (len(values) - 2)


def _count_markers(texts: list[str], markers: list[str]) -> int:
    """Count how many texts contain any of the markers."""
    count = 0
    for text in texts:
        lower = text.lower()
        if any(m in lower for m in markers):
            count += 1
    return count


def _topic_keyword_entropy(texts: list[str], window_size: int = 3) -> float:
    """Jaccard distance between sliding windows of keyword sets."""
    if len(texts) < window_size + 1:
        return 0.0

    # Extract keywords (simple: split on non-alpha, filter short words)
    def keywords(text):
        words = set(re.findall(r"[a-z]{3,}", text.lower()))
        # Filter very common words
        stop = {
            "the",
            "and",
            "for",
            "that",
            "this",
            "with",
            "you",
            "are",
            "was",
            "have",
            "has",
            "not",
            "but",
            "can",
            "from",
            "they",
            "been",
            "will",
            "would",
            "could",
            "should",
            "about",
            "into",
            "more",
            "some",
            "like",
            "just",
            "also",
            "than",
            "them",
            "then",
            "when",
            "what",
            "which",
            "there",
            "their",
            "your",
            "all",
            "any",
            "each",
            "how",
        }
        return words - stop

    kw_sets = [keywords(t) for t in texts]
    distances = []

    for i in range(len(kw_sets) - window_size):
        w1 = set()
        for j in range(window_size):
            w1 |= kw_sets[i + j]
        w2 = set()
        for j in range(window_size):
            idx = i + j + 1
            if idx < len(kw_sets):
                w2 |= kw_sets[idx]
        if not w1 and not w2:
            continue
        union = w1 | w2
        if not union:
            continue
        intersection = w1 & w2
        distances.append(1 - len(intersection) / len(union))

    return sum(distances) / len(distances) if distances else 0.0


def extract_features():
    """Extract features for all sessions."""
    conn = get_writer()

    try:
        conn.execute("DELETE FROM session_features")

        sessions = conn.execute("SELECT session_id FROM sessions").fetchall()

        for (session_id,) in sessions:
            _extract_session_features(session_id, conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(sessions)


def _extract_session_features(session_id: str, conn):
    """Extract all features for a single session."""
    # Get user prompts (non-tool-result)
    user_rows = conn.execute(
        """
        SELECT user_text, user_text_length, timestamp_utc
        FROM raw_entries
        WHERE session_id = ? AND entry_type = 'user'
          AND NOT is_tool_result AND user_text_length > 0
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    # Get assistant responses
    assistant_rows = conn.execute(
        """
        SELECT text_length, input_tokens, output_tokens, tool_names, timestamp_utc
        FROM raw_entries
        WHERE session_id = ? AND entry_type = 'assistant'
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    # Get system entries for timing
    system_rows = conn.execute(
        """
        SELECT duration_ms FROM raw_entries
        WHERE session_id = ? AND entry_type = 'system' AND system_subtype = 'turn_duration'
    """,
        [session_id],
    ).fetchall()

    # Get session info
    session_info = conn.execute(
        """
        SELECT started_at, tool_use_count, agent_type FROM sessions WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()

    # Get sidechain count
    sidechain_count = conn.execute(
        """
        SELECT COUNT(*) FROM raw_entries
        WHERE session_id = ? AND is_sidechain = TRUE
    """,
        [session_id],
    ).fetchone()[0]

    # Get branch switches
    branches = conn.execute(
        """
        SELECT DISTINCT git_branch FROM raw_entries
        WHERE session_id = ? AND git_branch IS NOT NULL
    """,
        [session_id],
    ).fetchall()

    # Prompt metrics
    prompt_lengths = [r[1] for r in user_rows]
    prompt_texts = [r[0] for r in user_rows]
    avg_prompt_length = (
        sum(prompt_lengths) / len(prompt_lengths) if prompt_lengths else 0
    )
    prompt_length_trend = _linear_trend([float(x) for x in prompt_lengths])
    max_prompt_length = max(prompt_lengths) if prompt_lengths else 0

    # Response metrics
    response_lengths = [r[0] for r in assistant_rows if r[0] > 0]
    avg_response_length = (
        sum(response_lengths) / len(response_lengths) if response_lengths else 0
    )
    response_length_trend = _linear_trend([float(x) for x in response_lengths])
    response_length_cv = _coefficient_of_variation([float(x) for x in response_lengths])

    # Token totals.
    # For agents that report cumulative context tokens per call (e.g. OpenAI/Codex),
    # summing all entries would massively overcount. Use the peak (MAX) input token
    # value instead, which represents the unique context consumed across the session.
    agent_type = session_info[2] if session_info else None
    _cumulative_token_agents = {"codex"}
    if agent_type in _cumulative_token_agents:
        total_input = max((r[1] for r in assistant_rows), default=0)
    else:
        total_input = sum(r[1] for r in assistant_rows)
    total_output = sum(r[2] for r in assistant_rows)

    # Tool ratios
    all_tools = []
    for r in assistant_rows:
        if r[3]:
            all_tools.extend(r[3])
    total_tools = len(all_tools) if all_tools else 1

    def tool_ratio(category_tools):
        return sum(1 for t in all_tools if t in category_tools) / total_tools

    edit_write_ratio = tool_ratio(TOOL_CATEGORIES["edit_write"])
    read_grep_ratio = tool_ratio(TOOL_CATEGORIES["read_grep"])
    bash_ratio = tool_ratio(TOOL_CATEGORIES["bash"])
    task_ratio = tool_ratio(TOOL_CATEGORIES["task"])
    web_ratio = tool_ratio(TOOL_CATEGORIES["web"])
    unique_tools = len(set(all_tools))

    # Turn timing
    durations = [r[0] for r in system_rows if r[0] and r[0] > 0]
    avg_turn_duration = sum(durations) / len(durations) if durations else 0

    # Time of day / day of week (convert UTC to local time)
    hour_of_day = 0
    day_of_week = 0
    if session_info and session_info[0]:
        from datetime import datetime, timezone

        # Parse ISO timestamp string from SQLite
        started_utc = datetime.fromisoformat(session_info[0].replace("Z", "+00:00"))
        if started_utc.tzinfo is None:
            started_utc = started_utc.replace(tzinfo=timezone.utc)
        started_local = started_utc.astimezone()
        hour_of_day = started_local.hour
        day_of_week = started_local.weekday()

    # Correction / rephrasing / decision markers
    correction_count = _count_markers(prompt_texts, CORRECTION_MARKERS)
    correction_rate = correction_count / len(prompt_texts) if prompt_texts else 0
    rephrasing_count = _count_markers(prompt_texts, REPHRASING_MARKERS)
    decision_count = _count_markers(prompt_texts, DECISION_MARKERS)

    # All text for combined markers (user + assistant)
    all_texts = prompt_texts + [
        r[0]
        for r in conn.execute(
            """
        SELECT text_content FROM raw_entries
        WHERE session_id = ? AND entry_type = 'assistant' AND text_length > 0
    """,
            [session_id],
        ).fetchall()
    ]
    decision_count += _count_markers(
        [
            r[0]
            for r in conn.execute(
                """
            SELECT text_content FROM raw_entries
            WHERE session_id = ? AND entry_type = 'assistant' AND text_length > 0
        """,
                [session_id],
            ).fetchall()
        ],
        DECISION_MARKERS,
    )

    # Topic entropy (from user prompts)
    topic_entropy = _topic_keyword_entropy(prompt_texts)

    # Sidechain ratio
    total_entries = conn.execute(
        "SELECT COUNT(*) FROM raw_entries WHERE session_id = ?", [session_id]
    ).fetchone()[0]
    sidechain_ratio = sidechain_count / total_entries if total_entries > 0 else 0

    # Abandoned detection: session with <=1 user prompt
    abandoned = len(prompt_texts) <= 1

    # PR link detection
    has_pr = any(
        "pull request" in t.lower()
        or "pr #" in t.lower()
        or "github.com" in t.lower()
        and "/pull/" in t.lower()
        for t in all_texts
    )

    # Branch switches
    branch_switch_count = max(0, len(branches) - 1)

    # Prompt length oscillation
    prompt_oscillation = _oscillation_score([float(x) for x in prompt_lengths])

    # API errors (from assistant entries with error markers)
    api_errors = conn.execute(
        """
        SELECT COUNT(*) FROM raw_entries
        WHERE session_id = ? AND entry_type = 'system' AND system_subtype = 'api_error'
    """,
        [session_id],
    ).fetchone()[0]

    conn.execute(
        """
        INSERT OR REPLACE INTO session_features VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """,
        [
            session_id,
            avg_prompt_length,
            prompt_length_trend,
            max_prompt_length,
            avg_response_length,
            response_length_trend,
            response_length_cv,
            total_input,
            total_output,
            edit_write_ratio,
            read_grep_ratio,
            bash_ratio,
            task_ratio,
            web_ratio,
            unique_tools,
            avg_turn_duration,
            hour_of_day,
            day_of_week,
            correction_count,
            correction_rate,
            rephrasing_count,
            decision_count,
            topic_entropy,
            sidechain_count,
            sidechain_ratio,
            abandoned,
            has_pr,
            branch_switch_count,
            prompt_oscillation,
            api_errors,
            0,  # subagent_spawn_count
            0,  # subagent_tool_diversity
            0,  # subagent_error_rate
            0,  # bash_heartbeat_count
        ],
    )
