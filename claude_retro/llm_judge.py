"""LLM-as-Observer: session analysis via Anthropic-compatible API."""

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from anthropic import Anthropic

from .db import get_conn

# How many sessions to judge in parallel
CONCURRENCY = 12

# Defaults — override with ANTHROPIC_BASE_URL / ANTHROPIC_API_KEY / CLAUDE_RETRO_MODEL
_DEFAULT_BASE_URL = "http://localhost:8082"
_DEFAULT_MODEL = "haiku"


def _get_client() -> Anthropic:
    return Anthropic(
        base_url=os.environ.get("ANTHROPIC_BASE_URL", _DEFAULT_BASE_URL),
        api_key=os.environ.get("ANTHROPIC_API_KEY", "unused"),
    )


def call_claude(prompt: str) -> str:
    """Call the Anthropic-compatible API. Return response text."""
    client = _get_client()
    model = os.environ.get("CLAUDE_RETRO_MODEL", _DEFAULT_MODEL)
    resp = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


def build_session_summary(session_id: str, conn) -> tuple[str, int]:
    """Build a compressed session summary for LLM analysis.

    Includes first prompt in full, all user messages in full,
    tool names only (no verbose results), tool errors flagged,
    and relative timestamps.

    Returns (summary_text, work_turn_count) where work_turn_count is the
    total number of meaningful action rounds (user messages + assistant
    tool-call batches), which is what the LLM should evaluate for productivity.
    """
    from datetime import datetime

    def _parse_ts(s):
        """Parse ISO timestamp string, handling trailing Z."""
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    entries = conn.execute(
        """
        SELECT entry_type, timestamp_utc, user_text, tool_names,
               is_tool_result, tool_result_error, system_subtype, duration_ms
        FROM raw_entries
        WHERE session_id = ? AND NOT is_sidechain
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    if not entries:
        return "", 0

    # Find first timestamp for relative times
    # SQLite returns timestamps as strings, so parse them
    first_ts = _parse_ts(entries[0][1])
    lines = []
    is_first_user = True
    turn_num = 0

    for (
        entry_type,
        ts_str,
        user_text,
        tool_names,
        is_tool_result,
        tool_error,
        sys_sub,
        dur_ms,
    ) in entries:
        elapsed = ""
        if ts_str and first_ts:
            ts = _parse_ts(ts_str)
            delta = (ts - first_ts).total_seconds()
            if delta >= 60:
                elapsed = f"[+{delta / 60:.0f}m] "
            elif delta > 0:
                elapsed = f"[+{delta:.0f}s] "

        if entry_type == "user" and not is_tool_result and user_text:
            turn_num += 1
            if is_first_user:
                lines.append(f"{elapsed}TURN {turn_num} [user prompt]:\n{user_text}")
                is_first_user = False
            else:
                lines.append(f"{elapsed}TURN {turn_num} [user prompt]:\n{user_text}")

        elif entry_type == "assistant" and tool_names:
            turn_num += 1
            # tool_names from DB is a comma-separated string
            if isinstance(tool_names, str):
                tools_str = tool_names
            else:
                tools_str = ", ".join(tool_names)
            lines.append(f"{elapsed}TURN {turn_num} [assistant tools: {tools_str}]")

        elif entry_type == "user" and is_tool_result:
            if tool_error:
                lines.append(f"{elapsed}TOOL RESULT: **ERROR**")

        elif entry_type == "system" and sys_sub:
            if sys_sub == "api_error":
                lines.append(f"{elapsed}SYSTEM: API error")

    return "\n".join(lines), turn_num


_OUTCOME_PROMPT = """\
You are analyzing a Claude Code session transcript. Evaluate the outcome and prompt quality.

SESSION TRANSCRIPT:
{summary}

Respond with ONLY a JSON object (no markdown, no backticks):
{{
  "outcome": "completed" | "partially_completed" | "failed" | "abandoned" | "exploratory",
  "outcome_confidence": 0.0-1.0,
  "outcome_reasoning": "brief explanation",
  "prompt_clarity": 0.0-1.0,
  "prompt_completeness": 0.0-1.0,
  "prompt_missing": ["list of things missing or underspecified in the initial prompt"],
  "prompt_summary": "one sentence summary of what the user wanted"
}}

Definitions:
- completed: the task was finished successfully
- partially_completed: some progress but not fully done
- failed: attempted but did not succeed
- abandoned: user gave up or session ended abruptly with minimal progress
- exploratory: no specific goal, just exploring/reading code
- prompt_clarity: how clear and unambiguous the initial request was
- prompt_completeness: how much context/detail the initial prompt provided
- prompt_missing: specific things the user could have included upfront
- prompt_summary: ALWAYS provide a one-sentence summary, even for complex sessions"""


_TRAJECTORY_PROMPT = """\
You are analyzing a Claude Code session transcript. Evaluate the interaction trajectory.

SESSION TRANSCRIPT:
{summary}

This session has {turn_count} turns total. Each "TURN N" in the transcript is one turn — this includes
both user prompts and assistant tool-call rounds (since Claude works autonomously between user messages).

Respond with ONLY a JSON object (no markdown, no backticks):
{{
  "trajectory_summary": "2-3 sentence narrative of how the session evolved",
  "underspecified_parts": [
    {{"aspect": "what was underspecified", "impact": "what problem it caused"}}
  ],
  "misalignment_count": 0,
  "misalignments": [
    {{"turn": 1, "description": "what Claude did wrong (reference the assistant turn where the mistake happened)"}}
  ],
  "correction_count": 0,
  "corrections": [
    {{"turn": 2, "type": "clarification|redirect|fix", "description": "what the user said to fix it (reference the user turn where the correction happened)"}}
  ],
  "productive_turns": 0,
  "waste_turns": 0,
  "productivity_ratio": 0.0-1.0,
  "waste_breakdown": {{"misalignment": 0, "errors": 0, "rework": 0}}
}}

IMPORTANT rules:
- A "turn" is any TURN N in the transcript (user prompt OR assistant tool-call batch).
- productive_turns + waste_turns MUST equal {turn_count}.
- A turn is "productive" if it advanced the task (useful tool call, correct edit, meaningful progress).
- A turn is "waste" if it was wrong (wrong file, bad edit, tool error), redundant (re-doing work),
  or caused by a misalignment (user had to correct Claude's direction).
- If Claude made a tool call that produced an error, that turn is waste.
- If Claude went down the wrong path for several turns before being corrected, those turns are waste.
- misalignment_count must equal the length of the misalignments array.
- correction_count must equal the length of the corrections array.
- For misalignments: "turn" is the TURN N where Claude made the mistake (always an assistant turn).
- For corrections: "turn" is the TURN N where the user corrected it (always a user turn, which comes AFTER the misalignment).
- A misalignment on turn 5 and its correction on turn 6 means Claude went wrong at turn 5, user fixed it at turn 6. They must NOT have the same turn number.
- CRITICAL: productive_turns + waste_turns MUST equal exactly {turn_count}. Do not return fewer.

Focus on:
- Points where the human had to re-explain or clarify
- Whether Claude got stuck in loops or went off-track
- Tool calls that failed or were unnecessary
- Moments of alignment (things that worked well)"""


def _parse_json_response(text: str) -> dict:
    """Parse JSON from LLM response, handling common formatting issues."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)
    return json.loads(text)


def analyze_outcome(session_id: str, summary: str) -> dict:
    """Call LLM for outcome/prompt quality analysis."""
    prompt = _OUTCOME_PROMPT.format(summary=summary)
    raw = call_claude(prompt)
    try:
        parsed = _parse_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        parsed = {
            "outcome": "unknown",
            "outcome_confidence": 0.0,
            "outcome_reasoning": f"Failed to parse: {raw[:200]}",
            "prompt_clarity": 0.0,
            "prompt_completeness": 0.0,
            "prompt_missing": [],
            "prompt_summary": "",
        }
    parsed["_raw"] = raw
    return parsed


def analyze_trajectory(session_id: str, summary: str, turn_count: int = 0) -> dict:
    """Call LLM for trajectory analysis."""
    prompt = _TRAJECTORY_PROMPT.format(summary=summary, turn_count=turn_count)
    raw = call_claude(prompt)
    try:
        parsed = _parse_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        parsed = {
            "trajectory_summary": f"Failed to parse: {raw[:200]}",
            "underspecified_parts": [],
            "misalignment_count": 0,
            "misalignments": [],
            "correction_count": 0,
            "corrections": [],
            "productive_turns": 0,
            "waste_turns": 0,
            "productivity_ratio": 0.0,
            "waste_breakdown": {"misalignment": 0, "errors": 0, "rework": 0},
        }
    parsed["_raw"] = raw
    return parsed


_COMBINED_PROMPT = """\
You are analyzing a Claude Code session transcript. Evaluate both the outcome and trajectory.

SESSION TRANSCRIPT:
{summary}

This session has {turn_count} turns total. Each "TURN N" in the transcript is one turn.

Respond with ONLY a JSON object (no markdown, no backticks):
{{
  "outcome": "completed" | "partially_completed" | "failed" | "abandoned" | "exploratory",
  "outcome_confidence": 0.0-1.0,
  "outcome_reasoning": "brief explanation",
  "prompt_clarity": 0.0-1.0,
  "prompt_completeness": 0.0-1.0,
  "prompt_missing": ["list of things missing or underspecified in the initial prompt"],
  "prompt_summary": "one sentence summary of what the user wanted",
  "trajectory_summary": "2-3 sentence narrative of how the session evolved",
  "underspecified_parts": [
    {{"aspect": "what was underspecified", "impact": "what problem it caused"}}
  ],
  "misalignment_count": 0,
  "misalignments": [
    {{"turn": 1, "description": "what Claude did wrong"}}
  ],
  "correction_count": 0,
  "corrections": [
    {{"turn": 2, "type": "clarification|redirect|fix", "description": "what the user said to fix it"}}
  ],
  "productive_turns": 0,
  "waste_turns": 0,
  "productivity_ratio": 0.0-1.0,
  "waste_breakdown": {{"misalignment": 0, "errors": 0, "rework": 0}}
}}

Rules:
- outcome: completed=finished successfully, partially_completed=some progress, failed=didn't succeed, abandoned=gave up, exploratory=no specific goal
- productive_turns + waste_turns MUST equal {turn_count}
- A turn is "waste" if wrong, redundant, or caused by misalignment
- misalignment_count must equal length of misalignments array
- correction_count must equal length of corrections array"""


def analyze_combined(session_id: str, summary: str, turn_count: int = 0) -> dict:
    """Single LLM call for both outcome and trajectory analysis."""
    prompt = _COMBINED_PROMPT.format(summary=summary, turn_count=turn_count)
    raw = call_claude(prompt)
    try:
        parsed = _parse_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        parsed = {
            "outcome": "unknown",
            "outcome_confidence": 0.0,
            "outcome_reasoning": f"Failed to parse: {raw[:200]}",
            "prompt_clarity": 0.0,
            "prompt_completeness": 0.0,
            "prompt_missing": [],
            "prompt_summary": "",
            "trajectory_summary": "",
            "underspecified_parts": [],
            "misalignment_count": 0,
            "misalignments": [],
            "correction_count": 0,
            "corrections": [],
            "productive_turns": 0,
            "waste_turns": 0,
            "productivity_ratio": 0.0,
            "waste_breakdown": {"misalignment": 0, "errors": 0, "rework": 0},
        }
    parsed["_raw"] = raw
    return parsed


def _build_record(session_id, summary, turn_count=0):
    """Run combined analysis for a single session, return record dict."""
    result = analyze_combined(session_id, summary, turn_count)
    # Split into outcome/trajectory for compatibility with record schema
    outcome = result
    trajectory = result

    productive = trajectory.get("productive_turns", 0)
    waste = trajectory.get("waste_turns", 0)
    total = productive + waste
    misalign_count = trajectory.get("misalignment_count", 0)

    # Validate: if LLM returned fewer turns than expected, adjust
    if turn_count > 0 and total < turn_count:
        # LLM undercounted — distribute the missing turns proportionally
        if total > 0:
            productive = round(productive * turn_count / total)
            waste = turn_count - productive
        else:
            # LLM returned 0/0 — estimate from misalignment count
            waste = min(misalign_count * 2, turn_count)
            productive = turn_count - waste
        total = productive + waste

    # Validate: if misalignment count is high but waste is near-zero, fix
    if misalign_count >= 3 and total > 0 and waste / total < 0.1:
        # Each misalignment implies at least ~1 wasted turn
        min_waste = min(misalign_count, turn_count or total)
        if waste < min_waste:
            waste = min_waste
            productive = max(0, total - waste)

    ratio = (
        productive / total if total > 0 else trajectory.get("productivity_ratio", 0.0)
    )

    return {
        "session_id": session_id,
        "outcome": outcome.get("outcome", "unknown"),
        "outcome_confidence": outcome.get("outcome_confidence", 0.0),
        "outcome_reasoning": outcome.get("outcome_reasoning", ""),
        "prompt_clarity": outcome.get("prompt_clarity", 0.0),
        "prompt_completeness": outcome.get("prompt_completeness", 0.0),
        "prompt_missing": json.dumps(outcome.get("prompt_missing", [])),
        "prompt_summary": outcome.get("prompt_summary", ""),
        "trajectory_summary": trajectory.get("trajectory_summary", ""),
        "underspecified_parts": json.dumps(trajectory.get("underspecified_parts", [])),
        "misalignment_count": misalign_count,
        "misalignments": json.dumps(trajectory.get("misalignments", [])),
        "correction_count": trajectory.get("correction_count", 0),
        "corrections": json.dumps(trajectory.get("corrections", [])),
        "productive_turns": productive,
        "waste_turns": waste,
        "productivity_ratio": ratio,
        "waste_breakdown": json.dumps(trajectory.get("waste_breakdown", {})),
        "raw_analysis_1": outcome.get("_raw", ""),
        "raw_analysis_2": "",
    }


def _judge_one(session_id, summary, turn_count):
    """Judge a single session given its pre-built summary. Returns (session_id, record) or (session_id, error)."""
    try:
        record = _build_record(session_id, summary, turn_count)
        return (session_id, record)
    except Exception as e:
        return (session_id, e)


def judge_session(session_id: str, conn) -> dict:
    """Build summary and run both analyses for a session."""
    from .db import get_writer

    summary, work_turns = build_session_summary(session_id, conn)
    if not summary:
        return None

    record = _build_record(session_id, summary, work_turns)

    wconn = get_writer()
    wconn.execute("DELETE FROM session_judgments WHERE session_id = ?", [session_id])
    cols = ", ".join(record.keys())
    placeholders = ", ".join(["?"] * len(record))
    wconn.execute(
        f"INSERT INTO session_judgments ({cols}) VALUES ({placeholders})",
        list(record.values()),
    )
    wconn.commit()

    return record


def judge_sessions(
    force: bool = False, concurrency: int = CONCURRENCY, progress_callback=None
) -> int:
    """Judge all unjudged sessions (incremental). Returns count judged.

    Runs `concurrency` sessions in parallel (each session runs its 2 LLM
    calls in parallel too, so peak subprocess count is 2*concurrency).

    progress_callback(done, total, ok, errors) is called after each session completes.
    """
    from .db import get_writer

    conn = get_conn()       # reader for SELECT queries
    wconn = get_writer()    # writer for INSERT/DELETE

    # Only judge sessions with >= 1 turn (0-turn sessions are trivial Q&A or meta-analysis)
    min_turns = 1

    if force:
        session_rows = conn.execute(
            "SELECT session_id, user_prompt_count FROM sessions WHERE turn_count >= ? ORDER BY started_at",
            [min_turns],
        ).fetchall()
    else:
        session_rows = conn.execute("""
            SELECT s.session_id, s.user_prompt_count
            FROM sessions s
            LEFT JOIN session_judgments j ON s.session_id = j.session_id
            WHERE j.session_id IS NULL AND s.turn_count >= ?
            ORDER BY s.started_at
        """, [min_turns]).fetchall()

    total = len(session_rows)
    if total == 0:
        if progress_callback:
            progress_callback(0, 0, 0, 0)
        return 0

    # Pre-build all summaries (fast, pure DB reads) so the thread pool
    # only does the slow LLM calls and doesn't contend on DuckDB.
    summaries = {}
    turn_counts = {}
    for sid, upc in session_rows:
        summary, work_turns = build_session_summary(sid, conn)
        summaries[sid] = summary
        turn_counts[sid] = work_turns

    # Filter out empty summaries
    work = [
        (sid, summaries[sid], turn_counts[sid])
        for sid, _ in session_rows
        if summaries[sid]
    ]
    total = len(work)
    if total == 0:
        if progress_callback:
            progress_callback(0, 0, 0, 0)
        return 0

    print(f"  Judging {total} sessions ({concurrency} parallel)...")
    if progress_callback:
        progress_callback(0, total, 0, 0)

    count = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(_judge_one, sid, summary, tc): sid for sid, summary, tc in work
        }
        for future in as_completed(futures):
            sid, result = future.result()
            if isinstance(result, Exception):
                errors += 1
                print(f"  Warning: {sid[:12]}...: {result}", file=sys.stderr)
            else:
                # Write to DB using writer connection
                wconn.execute(
                    "DELETE FROM session_judgments WHERE session_id = ?", [sid]
                )
                cols = ", ".join(result.keys())
                placeholders = ", ".join(["?"] * len(result))
                wconn.execute(
                    f"INSERT INTO session_judgments ({cols}) VALUES ({placeholders})",
                    list(result.values()),
                )
                wconn.commit()
                count += 1

            done = count + errors
            if progress_callback:
                progress_callback(done, total, count, errors)
            if done % 10 == 0 or done == total:
                print(f"  Progress: {done}/{total} ({count} ok, {errors} errors)")

    return count
