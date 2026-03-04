"""Skill tree assessment: detect demonstrated and opportunity levels per dimension."""

from .config import (
    SKILL_ACCEPTANCE_CRITERIA,
    SKILL_COMPACT_FOCUS,
    SKILL_CONTEXT_KEYWORDS,
    SKILL_DIMENSIONS,
    SKILL_INIT_COMMANDS,
    SKILL_NUDGES,
    SKILL_PROMPT_REFS,
    SKILL_ROOT_CAUSE,
    SKILL_SESSION_RESUME,
    SKILL_TEST_COMMANDS,
    SKILL_THINKING_TRIGGERS,
)
from .db import get_writer


def assess_skills() -> int:
    """Assess skill levels for all sessions. Returns count of sessions assessed."""
    conn = get_writer()

    try:
        conn.execute("DELETE FROM session_skills")

        sessions = conn.execute("SELECT session_id FROM sessions").fetchall()

        for (session_id,) in sessions:
            _assess_session(session_id, conn)

        # Recompute aggregate profile
        _compute_skill_profile(conn)
        # Generate nudges from gaps
        _generate_skill_nudges(conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(sessions)


def _assess_session(session_id: str, conn):
    """Assess all 10 skill dimensions for a single session."""
    # Gather session data
    data = _gather_session_data(session_id, conn)
    if not data:
        return

    # Run each dimension detector
    d1 = _detect_context_mgmt(data)
    d2 = _detect_planning(data)
    d3 = _detect_prompt_craft(data)
    d4 = _detect_claude_md(data)
    d5 = _detect_tool_leverage(data)
    d6 = _detect_verification(data)
    d7 = _detect_git_workflow(data)
    d8 = _detect_error_recovery(data)
    d9 = _detect_session_strategy(data)
    d10 = _detect_codebase_design(data)

    # Count dimensions with actual signal for confidence
    dims = [d1, d2, d3, d4, d5, d6, d7, d8, d9, d10]
    signal_count = sum(1 for lev, opp in dims if lev > 0 or opp > 0)
    confidence = signal_count / 10.0

    conn.execute(
        """
        INSERT OR REPLACE INTO session_skills VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """,
        [
            session_id,
            d1[0],
            d1[1],
            d2[0],
            d2[1],
            d3[0],
            d3[1],
            d4[0],
            d4[1],
            d5[0],
            d5[1],
            d6[0],
            d6[1],
            d7[0],
            d7[1],
            d8[0],
            d8[1],
            d9[0],
            d9[1],
            d10[0],
            d10[1],
            confidence,
            None,  # assessed_at uses DEFAULT
        ],
    )


def _gather_session_data(session_id: str, conn) -> dict | None:
    """Gather all data needed for skill detection."""
    session = conn.execute(
        """
        SELECT session_id, duration_seconds, user_prompt_count, tool_use_count,
               tool_error_count, turn_count, first_prompt, trajectory
        FROM sessions WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()
    if not session:
        return None

    features_cursor = conn.execute(
        "SELECT * FROM session_features WHERE session_id = ?", [session_id]
    )
    features = features_cursor.fetchone()
    feature_cols = [d[0] for d in features_cursor.description] if features else []
    feat = dict(zip(feature_cols, features)) if features else {}

    user_texts = [
        r[0]
        for r in conn.execute(
            """
        SELECT user_text FROM raw_entries
        WHERE session_id = ? AND entry_type = 'user'
          AND NOT is_tool_result AND user_text_length > 0
        ORDER BY timestamp_utc
    """,
            [session_id],
        ).fetchall()
    ]

    tool_names = [
        r[0]
        for r in conn.execute(
            """
        SELECT tool_name FROM session_tool_usage WHERE session_id = ?
    """,
            [session_id],
        ).fetchall()
    ]

    tool_usage = {}
    for r in conn.execute(
        """
        SELECT tool_name, use_count, error_count
        FROM session_tool_usage WHERE session_id = ?
    """,
        [session_id],
    ).fetchall():
        tool_usage[r[0]] = {"use_count": r[1], "error_count": r[2]}

    # Get all assistant tool_names arrays for ordering analysis
    assistant_tools = [
        r[0]
        for r in conn.execute(
            """
        SELECT tool_names FROM raw_entries
        WHERE session_id = ? AND entry_type = 'assistant' AND tool_names IS NOT NULL
        ORDER BY timestamp_utc
    """,
            [session_id],
        ).fetchall()
    ]

    # Flatten tool sequence
    tool_sequence = []
    for tools in assistant_tools:
        if tools:
            tool_sequence.extend(tools)

    # Count files modified (Edit/Write uses)
    files_modified = sum(
        tool_usage.get(t, {}).get("use_count", 0)
        for t in ["Edit", "Write", "NotebookEdit"]
    )

    judgment = conn.execute(
        """
        SELECT prompt_clarity, prompt_completeness, correction_count,
               productivity_ratio, outcome
        FROM session_judgments WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()

    return {
        "session_id": session_id,
        "duration": session[1] or 0,
        "user_prompt_count": session[2] or 0,
        "tool_use_count": session[3] or 0,
        "tool_error_count": session[4] or 0,
        "turn_count": session[5] or 0,
        "first_prompt": session[6] or "",
        "trajectory": session[7] or "unknown",
        "features": feat,
        "user_texts": user_texts,
        "tool_names": tool_names,
        "tool_usage": tool_usage,
        "tool_sequence": tool_sequence,
        "files_modified": files_modified,
        "judgment": {
            "prompt_clarity": judgment[0] if judgment else None,
            "prompt_completeness": judgment[1] if judgment else None,
            "correction_count": judgment[2] if judgment else None,
            "productivity_ratio": judgment[3] if judgment else None,
            "outcome": judgment[4] if judgment else None,
        },
    }


def _has_any(texts: list[str], markers: list[str]) -> bool:
    """Check if any text contains any marker (case-insensitive)."""
    for text in texts:
        lower = text.lower()
        if any(m.lower() in lower for m in markers):
            return True
    return False


def _has_numbered_steps(texts: list[str]) -> bool:
    """Check if texts contain actual numbered planning steps (not version numbers etc).

    Looks for patterns like "step 1:", "1) ...", or lines starting with "1. " at
    the beginning of a line/text, requiring at least 2 consecutive numbers.
    """
    import re

    for text in texts:
        lower = text.lower()
        # "step N" is unambiguous planning
        if re.search(r"\bstep\s+\d", lower):
            return True
        # Multiple consecutive numbered items: "1. ... 2. ..." or "1) ... 2) ..."
        # Require at least 2 to avoid matching casual "1." in prose
        nums_found = re.findall(r"(?:^|\n)\s*(\d+)[.)]\s", text)
        if len(nums_found) >= 2:
            return True
    return False


# ===== DIMENSION DETECTORS =====
# Each returns (demonstrated_level, opportunity_level)
# Level 0 = insufficient signal, 1-5 = skill levels


def _detect_context_mgmt(data: dict) -> tuple[int, int]:
    """D1: Context Window Management."""
    texts = data["user_texts"]
    feat = data["features"]
    level = 1  # baseline: using Claude at all
    opportunity = 0

    has_clear = _has_any(texts, ["/clear"])
    has_compact = _has_any(texts, ["/compact"])
    has_compact_focus = _has_any(texts, SKILL_COMPACT_FOCUS)
    has_context_cmd = _has_any(texts, SKILL_CONTEXT_KEYWORDS)
    entropy = feat.get("topic_keyword_entropy", 0)

    if has_compact or has_clear:
        level = 2
    if has_compact and has_compact_focus:
        level = 3
    if has_clear and has_compact:
        level = max(level, 3)

    # Opportunity detection
    if entropy > 0.5 and not has_clear and not has_compact:
        opportunity = 2  # high topic drift without context management
    elif data["duration"] > 1800 and not has_context_cmd:
        opportunity = 2  # long session without any context commands
    elif has_compact and not has_compact_focus and entropy > 0.3:
        opportunity = 3  # using /compact but without focus instructions

    return (level, opportunity)


def _detect_planning(data: dict) -> tuple[int, int]:
    """D2: Planning & Task Decomposition."""
    texts = data["user_texts"]
    level = 1
    opportunity = 0

    has_plan_mode = _has_any(texts, ["plan mode", "enterplanmode"])
    has_numbered = _has_numbered_steps(texts)
    has_task_tool = "Task" in data["tool_names"]
    has_spec = _has_any(texts, ["spec.md", "SPEC.md", "implementation plan"])

    if has_numbered or has_spec:
        level = 2
    if has_plan_mode:
        level = max(level, 3)
    if has_task_tool and has_plan_mode:
        level = max(level, 4)

    # Opportunity: many files modified without planning signals
    if data["files_modified"] >= 5 and not has_plan_mode and not has_numbered:
        opportunity = 3
    elif data["turn_count"] >= 10 and not has_numbered:
        opportunity = 2

    return (level, opportunity)


def _detect_prompt_craft(data: dict) -> tuple[int, int]:
    """D3: Prompt Craft."""
    texts = data["user_texts"]
    feat = data["features"]
    j = data["judgment"]
    level = 1
    opportunity = 0

    has_file_ref = _has_any(texts, SKILL_PROMPT_REFS)
    has_acceptance = _has_any(texts, SKILL_ACCEPTANCE_CRITERIA)
    has_thinking = _has_any(texts, SKILL_THINKING_TRIGGERS)
    has_error_context = _has_any(
        texts, ["stack trace", "traceback", "error:", "Error:"]
    )
    first_prompt_len = len(data["first_prompt"])
    correction_rate = feat.get("correction_rate", 0)

    if first_prompt_len > 200 or has_error_context:
        level = 2
    if has_file_ref or has_acceptance:
        level = max(level, 3)
    if has_thinking and has_acceptance:
        level = max(level, 4)

    # Supplement from LLM judgments
    if j["prompt_clarity"] is not None and j["prompt_clarity"] >= 0.8:
        level = max(level, 3)

    # Opportunity detection
    if correction_rate > 0.3 and not has_acceptance:
        opportunity = 3
    elif first_prompt_len < 100 and data["turn_count"] > 5:
        opportunity = 2
    elif j["prompt_clarity"] is not None and j["prompt_clarity"] < 0.5:
        opportunity = 2

    return (level, opportunity)


def _detect_claude_md(data: dict) -> tuple[int, int]:
    """D4: CLAUDE.md Configuration. Low signal from session data."""
    texts = data["user_texts"]
    level = 0  # default: insufficient data
    opportunity = 0

    has_init = _has_any(texts, SKILL_INIT_COMMANDS)

    if has_init:
        level = 2

    return (level, opportunity)


def _detect_tool_leverage(data: dict) -> tuple[int, int]:
    """D5: Tool & Feature Leverage."""
    feat = data["features"]
    tool_names = data["tool_names"]
    level = 1
    opportunity = 0

    unique_tools = feat.get("unique_tools_used", 0)
    bash_ratio = feat.get("bash_ratio", 0)
    task_ratio = feat.get("task_ratio", 0)
    # Standard Claude Code tools — anything not in this set is likely MCP/custom
    _STANDARD_TOOLS = {
        "Edit",
        "Write",
        "Read",
        "Grep",
        "Glob",
        "Bash",
        "Task",
        "WebFetch",
        "WebSearch",
        "NotebookEdit",
        "AskUserQuestion",
        "EnterPlanMode",
        "ExitPlanMode",
        "Skill",
        "TaskCreate",
        "TaskUpdate",
        "TaskList",
        "TaskGet",
        "TaskOutput",
        "TaskStop",
        "ListMcpResourcesTool",
        "ReadMcpResourceTool",
    }
    has_mcp = any(
        t.startswith("mcp__") or (t not in _STANDARD_TOOLS and not t.startswith("Task"))
        for t in tool_names
    )

    if unique_tools >= 4:
        level = 2
    if unique_tools >= 6 and task_ratio > 0:
        level = 3
    if has_mcp:
        level = max(level, 4)

    # Opportunity: high bash ratio suggests not using dedicated tools
    if bash_ratio > 0.5 and unique_tools < 4:
        opportunity = 2
    elif unique_tools >= 4 and task_ratio == 0 and data["files_modified"] >= 3:
        opportunity = 3

    return (level, opportunity)


def _detect_verification(data: dict) -> tuple[int, int]:
    """D6: Verification & QA."""
    texts = data["user_texts"]
    tool_seq = data["tool_sequence"]
    feat = data["features"]
    level = 1
    opportunity = 0

    # Check for test commands in Bash tool calls or user prompts
    all_texts = texts + [data["first_prompt"]]
    has_test_mention = _has_any(all_texts, SKILL_TEST_COMMANDS)
    has_test_run = _has_any(
        all_texts, ["run the test", "run tests", "npm test", "pytest"]
    )
    # Detect test-first ordering: only counts if we ALSO have test signal
    # (Bash alone before Edit is not test-first — could be mkdir, git, etc.)
    test_first = False
    if tool_seq and (has_test_mention or has_test_run):
        first_bash_idx = None
        first_edit_idx = None
        for i, t in enumerate(tool_seq):
            if t == "Bash" and first_bash_idx is None:
                first_bash_idx = i
            if t in ("Edit", "Write") and first_edit_idx is None:
                first_edit_idx = i
        if first_bash_idx is not None and first_edit_idx is not None:
            test_first = first_bash_idx < first_edit_idx

    if has_test_mention or has_test_run:
        level = 2
    if test_first:
        level = max(level, 3)

    # Opportunity: edits without testing
    edit_write = feat.get("edit_write_ratio", 0)
    if edit_write > 0.2 and not has_test_mention:
        opportunity = 2
    elif has_test_mention and not test_first and data["files_modified"] >= 3:
        opportunity = 3

    return (level, opportunity)


def _detect_git_workflow(data: dict) -> tuple[int, int]:
    """D7: Git & Collaboration Workflow."""
    texts = data["user_texts"]
    feat = data["features"]
    level = 1
    opportunity = 0

    has_commit = _has_any(texts, ["/commit"])
    has_gh = _has_any(texts, ["gh pr", "gh issue"])
    has_pr = feat.get("has_pr_link", False)
    has_worktree = _has_any(texts, ["git worktree", "worktree"])

    if has_commit:
        level = 2
    if has_gh or has_pr:
        level = max(level, 3)
    if has_worktree:
        level = max(level, 4)

    # Opportunity: session with edits but no commit
    if data["files_modified"] >= 3 and not has_commit and not has_pr:
        opportunity = 2

    return (level, opportunity)


def _detect_error_recovery(data: dict) -> tuple[int, int]:
    """D8: Error Recovery & Debugging."""
    texts = data["user_texts"]
    feat = data["features"]
    level = 1
    opportunity = 0

    has_error_context = _has_any(
        texts, ["stack trace", "traceback", "error:", "Error:", "exception"]
    )
    has_root_cause = _has_any(texts, SKILL_ROOT_CAUSE)
    has_checkpoint = _has_any(
        texts, ["checkpoint", "git stash", "save state", "rewind"]
    )
    correction_count = feat.get("correction_count", 0)

    if has_error_context:
        level = 2
    if has_root_cause:
        level = max(level, 3)
    if has_checkpoint:
        level = max(level, 4)

    # Opportunity: many corrections without root cause analysis
    if correction_count >= 3 and not has_root_cause:
        opportunity = 3
    elif correction_count >= 1 and not has_error_context:
        opportunity = 2

    return (level, opportunity)


def _detect_session_strategy(data: dict) -> tuple[int, int]:
    """D9: Session Strategy & Parallelization."""
    texts = data["user_texts"]
    feat = data["features"]
    level = 1
    opportunity = 0

    duration = data["duration"]
    turn_count = data["turn_count"]
    entropy = feat.get("topic_keyword_entropy", 0)
    has_resume = _has_any(texts, SKILL_SESSION_RESUME)
    has_background = _has_any(
        texts,
        [
            "background agent",
            "run in background",
            "parallel session",
            "multiple sessions",
            "headless",
            "run_in_background",
        ],
    )

    if duration < 1800 and turn_count <= 20:
        level = 2  # focused session
    if has_resume:
        level = max(level, 3)
    if has_background:
        level = max(level, 4)

    # Opportunity: long unfocused session
    if duration > 3600 and entropy > 0.5:
        opportunity = 2
    elif duration > 1800 and turn_count > 20 and not has_resume:
        opportunity = 3

    return (level, opportunity)


def _detect_codebase_design(data: dict) -> tuple[int, int]:
    """D10: Codebase Design for Agents. Minimal signal from session data."""
    return (0, 0)


# ===== AGGREGATION =====


def _compute_skill_profile(conn):
    """Compute aggregate skill profile across recent sessions."""
    # Get last 100 sessions with skill assessments, ordered by recency
    cursor = conn.execute("""
        SELECT sk.*, s.started_at
        FROM session_skills sk
        JOIN sessions s ON sk.session_id = s.session_id
        ORDER BY s.started_at DESC
        LIMIT 100
    """)
    rows = cursor.fetchall()

    if not rows:
        return

    cols = [d[0] for d in cursor.description]

    # Exponential decay weights (most recent = highest weight)
    n = len(rows)
    decay = 0.95
    weights = [decay**i for i in range(n)]
    total_weight = sum(weights)

    # Compute weighted average for each dimension
    scores = {}
    for dim_num in range(1, 11):
        dim_id = f"D{dim_num}"
        level_col = f"d{dim_num}_level"
        opp_col = f"d{dim_num}_opportunity"

        level_idx = cols.index(level_col)
        opp_idx = cols.index(opp_col)

        weighted_level = sum((rows[i][level_idx] or 0) * weights[i] for i in range(n))
        scores[dim_id] = weighted_level / total_weight if total_weight > 0 else 0

    # Identify top 3 gaps (largest opportunity - demonstrated delta)
    gap_scores = []
    for dim_num in range(1, 11):
        dim_id = f"D{dim_num}"
        level_col = f"d{dim_num}_level"
        opp_col = f"d{dim_num}_opportunity"

        level_idx = cols.index(level_col)
        opp_idx = cols.index(opp_col)

        # Average opportunity and level across recent sessions
        recent = rows[:20]  # focus on last 20 for gap detection
        avg_level = (
            sum((r[level_idx] or 0) for r in recent) / len(recent) if recent else 0
        )
        avg_opp = sum((r[opp_idx] or 0) for r in recent) / len(recent) if recent else 0

        gap = avg_opp - avg_level
        if gap > 0:
            gap_scores.append((dim_id, gap, avg_opp))

    gap_scores.sort(key=lambda x: -x[1])
    top_gaps = [g[0] for g in gap_scores[:3]]

    # Upsert profile
    conn.execute("DELETE FROM skill_profile")
    conn.execute(
        """
        INSERT INTO skill_profile (id, d1_score, d2_score, d3_score, d4_score,
            d5_score, d6_score, d7_score, d8_score, d9_score, d10_score,
            gap_1, gap_2, gap_3, session_count)
        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            round(scores.get("D1", 0), 2),
            round(scores.get("D2", 0), 2),
            round(scores.get("D3", 0), 2),
            round(scores.get("D4", 0), 2),
            round(scores.get("D5", 0), 2),
            round(scores.get("D6", 0), 2),
            round(scores.get("D7", 0), 2),
            round(scores.get("D8", 0), 2),
            round(scores.get("D9", 0), 2),
            round(scores.get("D10", 0), 2),
            top_gaps[0] if len(top_gaps) > 0 else None,
            top_gaps[1] if len(top_gaps) > 1 else None,
            top_gaps[2] if len(top_gaps) > 2 else None,
            n,
        ],
    )


def _generate_skill_nudges(conn):
    """Generate nudge suggestions from skill gaps."""
    conn.execute("DELETE FROM skill_nudges WHERE dismissed = FALSE")

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return

    cols = [d[0] for d in cursor.description]
    p = dict(zip(cols, profile))

    # For each gap dimension, generate a nudge
    for gap_key in ["gap_1", "gap_2", "gap_3"]:
        dim_id = p.get(gap_key)
        if not dim_id:
            continue

        dim_num = int(dim_id[1:])
        current_score = p.get(f"d{dim_num}_score", 0)
        current_level = int(current_score)
        target_level = current_level + 1

        # Look up nudge text
        nudge_key = (dim_id, target_level)
        nudge_text = SKILL_NUDGES.get(nudge_key)
        if not nudge_text:
            continue

        dim_name = SKILL_DIMENSIONS[dim_id]["name"]

        conn.execute(
            """
            INSERT INTO skill_nudges (dimension, current_level, target_level,
                nudge_text, evidence, frequency)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            [
                dim_id,
                current_level,
                target_level,
                nudge_text,
                f"{dim_name}: currently at L{current_level}, aiming for L{target_level}",
                1,
            ],
        )
