"""LLM-as-Observer: session analysis via Anthropic-compatible API."""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

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

    Includes user prompts in full, assistant reasoning snippets (first 200 chars),
    tool names with ok/error status, tool error details, and relative timestamps.

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
               is_tool_result, tool_result_error, system_subtype, duration_ms,
               text_content, input_tokens, output_tokens
        FROM raw_entries
        WHERE session_id = ? AND NOT is_sidechain
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    if not entries:
        return "", 0

    # Find first timestamp for relative times
    first_ts = _parse_ts(entries[0][1])
    lines = []
    turn_num = 0

    # Track token totals for cost estimate
    total_input_tokens = 0
    total_output_tokens = 0

    # We need to look ahead for tool results to annotate assistant turns
    # Build list of entries with index for look-ahead
    pending_tool_errors = []

    for idx, (
        entry_type,
        ts_str,
        user_text,
        tool_names,
        is_tool_result,
        tool_error,
        sys_sub,
        dur_ms,
        text_content,
        input_tokens,
        output_tokens,
    ) in enumerate(entries):
        total_input_tokens += input_tokens or 0
        total_output_tokens += output_tokens or 0

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
            lines.append(f"{elapsed}TURN {turn_num} [user prompt]:\n{user_text}")

        elif entry_type == "assistant":
            if tool_names:
                turn_num += 1
                # tool_names from DB is stored as a JSON array string
                if isinstance(tool_names, str):
                    try:
                        tools_list = json.loads(tool_names)
                    except (json.JSONDecodeError, ValueError):
                        tools_list = [t.strip() for t in tool_names.split(",")]
                else:
                    tools_list = list(tool_names)

                # Add assistant reasoning snippet if available
                snippet = ""
                if text_content:
                    snippet_text = text_content[:200].strip()
                    if snippet_text:
                        snippet = f'  "{snippet_text}{"..." if len(text_content) > 200 else ""}"'

                # Look ahead for tool results to annotate ok/error
                tool_statuses = []
                j = idx + 1
                while j < len(entries):
                    ne = entries[j]
                    if ne[0] == "user" and ne[4]:  # is_tool_result
                        if ne[5]:  # tool_result_error
                            err_text = (ne[8] or "")[:150].strip()  # text_content
                            tool_statuses.append(("error", err_text))
                        else:
                            tool_statuses.append(("ok", ""))
                        j += 1
                    else:
                        break

                # Build tool status annotations
                tool_parts = []
                for i, tool in enumerate(tools_list):
                    if i < len(tool_statuses):
                        status, err_text = tool_statuses[i]
                        if status == "error" and err_text:
                            tool_parts.append(f'{tool} (error: "{err_text}")')
                        elif status == "error":
                            tool_parts.append(f"{tool} (error)")
                        else:
                            tool_parts.append(f"{tool} (ok)")
                    else:
                        tool_parts.append(tool)

                if snippet:
                    lines.append(f"{elapsed}TURN {turn_num} [assistant]:{snippet}")
                    lines.append(f"  tools: {', '.join(tool_parts)}")
                else:
                    lines.append(
                        f"{elapsed}TURN {turn_num} [assistant tools: {', '.join(tool_parts)}]"
                    )
            elif text_content:
                # Assistant text without tools (explanation/reasoning)
                turn_num += 1
                snippet = text_content[:200].strip()
                lines.append(
                    f'{elapsed}TURN {turn_num} [assistant]: "{snippet}{"..." if len(text_content) > 200 else ""}"'
                )

        elif entry_type == "system" and sys_sub:
            if sys_sub == "api_error":
                lines.append(f"{elapsed}SYSTEM: API error")

    # Add token cost estimate at the top
    cost = _estimate_cost(total_input_tokens, total_output_tokens)
    header = f"SESSION STATS: {turn_num} turns, ~{total_input_tokens + total_output_tokens:,} tokens, ~${cost:.2f} estimated cost\n"

    return header + "\n".join(lines), turn_num


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Rough cost estimate based on typical Claude pricing."""
    # Approximate: $3/M input, $15/M output (Sonnet-class)
    return (input_tokens * 3 + output_tokens * 15) / 1_000_000


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
You are analyzing a Claude Code session transcript. Evaluate the outcome, trajectory, and write a narrative.

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
  "friction_categories": {{
    "wrong_approach": 0,
    "buggy_code": 0,
    "tone_mismatch": 0,
    "scope_creep": 0,
    "circular_debug": 0,
    "other": 0
  }},
  "productive_turns": 0,
  "waste_turns": 0,
  "productivity_ratio": 0.0-1.0,
  "waste_breakdown": {{"misalignment": 0, "errors": 0, "rework": 0}},
  "narrative": "3-4 paragraph story of what happened in this session. Be specific — reference actual prompts, tool calls, errors. Write in past tense, third person. Include what went well and what went wrong.",
  "what_worked": "1-2 sentences on what went well, with specific examples from the transcript",
  "what_failed": "1-2 sentences on what went wrong, with specific examples from the transcript. If nothing failed, say so.",
  "user_quote": "the most notable thing the user said, verbatim from the transcript (copy the exact text)",
  "claude_md_suggestion": "A specific CLAUDE.md rule that would prevent this session's friction or improve future sessions. Format as a single line starting with '- '. If the session was perfect, suggest a rule that reinforces what worked.",
  "claude_md_rationale": "Why this rule matters, referencing what happened in this session"
}}

Rules:
- outcome: completed=finished successfully, partially_completed=some progress, failed=didn't succeed, abandoned=gave up, exploratory=no specific goal
- productive_turns + waste_turns MUST equal {turn_count}
- A turn is "waste" if wrong, redundant, or caused by misalignment
- misalignment_count must equal length of misalignments array
- correction_count must equal length of corrections array
- friction_categories: Count misalignments by type. wrong_approach=Claude chose wrong strategy/method. buggy_code=Claude produced code with bugs/errors. tone_mismatch=Claude used wrong tone/style/framing for content. scope_creep=Claude added unrequested features/content. circular_debug=Claude kept retrying same failing approach. other=doesn't fit above.
- narrative: Write a SPECIFIC story. Don't say "the user asked Claude to do X". Say "the user asked Claude to fix the flaky test in auth.py". Reference actual filenames, error messages, tool names.
- user_quote: Copy the most interesting/revealing user message verbatim. Pick the one that best shows the user's intent or frustration.
- claude_md_suggestion: Must be actionable and specific. Bad: "Be more careful". Good: "- Always run tests after editing test files before reporting success"."""


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

    # Extract friction_categories from LLM result
    friction_cats = result.get("friction_categories", {})

    # Parse estimated cost from session summary header (~$X.XX estimated cost)
    cost_match = re.search(r'~\$(\d+\.\d+) estimated cost', summary)
    cost_usd = float(cost_match.group(1)) if cost_match else 0.0

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
        "narrative": result.get("narrative", ""),
        "what_worked": result.get("what_worked", ""),
        "what_failed": result.get("what_failed", ""),
        "user_quote": result.get("user_quote", ""),
        "claude_md_suggestion": result.get("claude_md_suggestion", ""),
        "claude_md_rationale": result.get("claude_md_rationale", ""),
        "friction_categories": json.dumps(friction_cats),
        "estimated_cost_usd": cost_usd,
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
        # No new sessions to judge, but still regenerate synthesis if we have data
        try:
            existing = conn.execute("SELECT COUNT(*) FROM session_judgments").fetchone()[0]
            if existing >= 3:
                print("  No new sessions to judge. Regenerating synthesis...")
                generate_synthesis()
        except Exception as e:
            print(f"  Warning: synthesis generation failed: {e}", file=sys.stderr)
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

    # Generate cross-session synthesis after judging (always, even if all errored)
    try:
        print("  Generating cross-session synthesis...")
        if progress_callback:
            progress_callback(total, total, count, errors)
        generate_synthesis()
    except Exception as e:
        print(f"  Warning: synthesis generation failed: {e}", file=sys.stderr)

    # Auto-apply CLAUDE.md suggestions to project files
    try:
        applied = auto_apply_claude_md_suggestions()
        if applied:
            print(f"  Auto-applied CLAUDE.md suggestions to {applied} project(s).")
    except Exception as e:
        print(f"  Warning: auto-apply CLAUDE.md failed: {e}", file=sys.stderr)

    return count


_SYNTHESIS_PROMPT = """\
You are analyzing a collection of Claude Code session analyses to produce a comprehensive user report.

SESSION SUMMARIES:
{session_data}

OVERALL STATS:
- Total sessions: {total_sessions}
- Completion rate: {completion_rate}
- Average productivity: {avg_productivity}
- Total hours: {total_hours}
- Sessions with misalignments: {misalignment_sessions}/{total_sessions}

SKILL PROFILE (current level 1-5 per dimension, based on session signals):
{skill_gaps}

Respond with ONLY a JSON object (no markdown, no backticks):
{{
  "at_a_glance": {{
    "whats_working": "2-3 sentences on patterns that lead to successful sessions",
    "whats_hindering": "2-3 sentences on recurring friction points",
    "quick_wins": "2-3 specific, actionable things the user could do today",
    "ambitious_workflows": "1-2 sentences on the most complex/impressive things the user has accomplished"
  }},
  "usage_narrative": "2-3 paragraph behavioral profile of how this user works with Claude Code. Be specific — reference actual project names, common patterns, time-of-day preferences. Write in second person ('you'). Mention specific strengths and blind spots.",
  "top_wins": [
    {{"title": "short title", "description": "1-2 sentences with specific examples from sessions"}}
  ],
  "top_friction": [
    {{"title": "short title", "description": "1-2 sentences explaining the pattern", "examples": ["specific example from a session"], "user_quote": "verbatim thing the user said that captures this friction (from the session data above, if available)"}}
  ],
  "claude_md_additions": [
    {{"rule": "the CLAUDE.md rule text (start with '- ')", "rationale": "why this matters", "evidence": "specific session examples that support this"}}
  ],
  "workflow_prompts": [
    {{
      "title": "short title (5-7 words)",
      "description": "1-2 sentences: why this pattern matters specifically for this user",
      "friction_pattern": "one of: wrong_approach|buggy_code|tone_mismatch|scope_creep|circular_debug",
      "paste_prompt": "MULTI-LINE LITERAL PROMPT the user pastes into Claude Code. Use [PLACEHOLDER] for user-specific fill-ins. Minimum 3 sentences. Should be copy-paste ready."
    }}
  ],
  "features_to_try": [
    {{
      "feature": "feature name (Custom Skills|Hooks|Task Agents|MCP Servers)",
      "why_for_you": "1-2 sentences specific to this user's session patterns",
      "setup_code": "actual shell command or JSON config to paste"
    }}
  ],
  "skill_dimension_nudges": {{
    "D1": "2-3 sentence evidence-backed insight for Context Window Management. Reference specific session patterns you see in the data (e.g. how many sessions, which projects). Include a concrete example phrase the user could say to Claude.",
    "D2": "same format for Planning & Task Decomposition",
    "D3": "same format for Prompt Craft",
    "D4": "same format for CLAUDE.md Config",
    "D5": "same format for Tool Leverage",
    "D6": "same format for Verification & QA",
    "D7": "same format for Git Workflow",
    "D8": "same format for Error Recovery",
    "D9": "same format for Session Strategy"
  }},
  "fun_headline": "A witty, humorous one-liner about a notable moment from the sessions (reference something specific)"
}}

Guidelines:
- top_wins: 2-4 items. Focus on impressive accomplishments and effective patterns.
- top_friction: 2-4 items. Focus on recurring problems, not one-off issues.
- claude_md_additions: 3-5 rules. Each must be specific and actionable. Bad: "Write better prompts". Good: "- When debugging, always include the full error message and stack trace in your first prompt".
- fun_headline: Be genuinely funny. Reference something specific from the sessions.
- usage_narrative: Paint a picture of the user's working style. Are they a debugger or a builder? Do they work in bursts or steady sessions? Do they give Claude freedom or micromanage?
- workflow_prompts: 3-4 items. Each paste_prompt must be IMMEDIATELY USABLE — multi-sentence, with [PLACEHOLDER] variables for user-specific parts. Model on this pattern: "Before implementing anything, let me set constraints: 1) I only own [THESE REPOS]. 2) Fix the implementation, not the tests. 3) [YOUR GOAL HERE]". Make them specific to the friction patterns you see in this user's data.
- features_to_try: 2-3 items. setup_code must be a real, copy-pasteable command or JSON snippet (e.g., actual hooks settings.json, actual mkdir command for skills).
- top_friction user_quote: pull verbatim text from "User said:" lines in the session data above. If multiple quotes fit, pick the most vivid one. If none available, omit the field or use empty string.
- skill_dimension_nudges: Include ALL 9 dimensions (D1-D9). For each, write a specific, evidence-backed 2-3 sentence insight based on the actual skill level shown and session patterns. Cite actual data ("across your X sessions on Y project, you..."). Include a copy-pasteable example phrase to use ("Try saying: 'Before we start, list the files we'll need to change'"). Focus on the gap between current level and next level. Do NOT write generic advice — reference THIS user's specific patterns from the session data."""


def _build_skill_gaps_summary(conn) -> str:
    """Build a skill profile summary string to pass to the synthesis LLM."""
    from .config import SKILL_DIMENSIONS

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    row = cursor.fetchone()
    if not row:
        return "(no skill profile available)"
    cols = [d[0] for d in cursor.description]
    profile = dict(zip(cols, row))

    # Build all-dimension profile
    lines = []
    for dim_num in range(1, 10):  # D1-D9 (D10 is Codebase Design, less relevant)
        dim_id = f"D{dim_num}"
        score = profile.get(f"d{dim_num}_score", 0) or 0
        level = int(score)
        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        name = dim_info.get("name", dim_id)
        lines.append(f"- {name} ({dim_id}): L{level} ({score:.2f}/5.0)")

    return "\n".join(lines) if lines else "(no skill profile available)"


def _update_skill_nudges_from_synthesis(conn, dim_nudges: dict):
    """Replace static skill nudge text with LLM-generated insights from synthesis."""
    for dim_id, nudge_text in dim_nudges.items():
        if not nudge_text or not dim_id.startswith("D"):
            continue
        # Update existing nudge for this dimension if it exists
        updated = conn.execute(
            "UPDATE skill_nudges SET nudge_text = ?, evidence = 'LLM-generated from session patterns' WHERE dimension = ? AND dismissed = 0",
            [nudge_text, dim_id],
        ).rowcount
        if updated == 0:
            # Insert a new nudge if none exists
            try:
                dim_num = int(dim_id[1:])
                conn.execute(
                    """INSERT INTO skill_nudges (dimension, current_level, target_level, nudge_text, evidence, frequency)
                       VALUES (?, 1, 2, ?, 'LLM-generated from session patterns', 1)""",
                    [dim_id, nudge_text],
                )
            except Exception:
                pass
    conn.commit()


def generate_synthesis():
    """Generate cross-session synthesis from all judged sessions. Stores result in synthesis table."""
    from .db import get_writer

    conn = get_conn()
    wconn = get_writer()

    # Gather session judgment data
    rows = conn.execute("""
        SELECT j.session_id, j.outcome, j.productivity_ratio, j.misalignment_count,
               j.narrative, j.what_worked, j.what_failed, j.user_quote,
               j.claude_md_suggestion, j.claude_md_rationale, j.prompt_summary,
               s.project_name, s.duration_seconds, s.turn_count
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
        ORDER BY s.started_at DESC
        LIMIT 50
    """).fetchall()

    if len(rows) < 3:
        return  # Not enough data for meaningful synthesis

    # Build session data summary for the LLM
    session_lines = []
    for r in rows:
        sid, outcome, prod, mis, narrative, worked, failed, quote, cmd_sug, cmd_rat, summary, project, dur, turns = r
        short_project = (project or "").replace("-Users-npow-code-", "").replace("-Users-npow-", "")
        line = f"- [{outcome}] {short_project}: {summary or '(no summary)'}"
        if narrative:
            line += f"\n  Narrative: {narrative[:200]}"
        if worked:
            line += f"\n  Worked: {worked[:100]}"
        if failed:
            line += f"\n  Failed: {failed[:100]}"
        if quote:
            line += f'\n  User said: "{quote[:100]}"'
        if cmd_sug:
            line += f"\n  CLAUDE.md suggestion: {cmd_sug[:100]}"
        line += f"\n  ({turns} turns, {dur // 60 if dur else 0}m, {prod:.0%} productive, {mis} misalignments)"
        session_lines.append(line)

    session_data = "\n\n".join(session_lines)

    # Gather stats
    stats = conn.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END) / COUNT(*) as comp_rate,
               AVG(j.productivity_ratio) as avg_prod,
               SUM(s.duration_seconds) / 3600.0 as total_hours,
               SUM(CASE WHEN j.misalignment_count > 0 THEN 1 ELSE 0 END) as mis_sessions
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """).fetchone()

    total, comp_rate, avg_prod, total_hours, mis_sessions = stats

    # Build skill gaps summary for the LLM
    skill_gaps = _build_skill_gaps_summary(conn)

    prompt = _SYNTHESIS_PROMPT.format(
        session_data=session_data,
        total_sessions=total,
        completion_rate=f"{comp_rate:.0%}" if comp_rate else "N/A",
        avg_productivity=f"{avg_prod:.0%}" if avg_prod else "N/A",
        total_hours=f"{total_hours:.1f}" if total_hours else "0",
        misalignment_sessions=mis_sessions or 0,
        skill_gaps=skill_gaps,
    )

    raw = call_claude(prompt)
    try:
        parsed = _parse_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        print(f"  Warning: synthesis JSON parse failed: {raw[:200]}", file=sys.stderr)
        return

    # Store in synthesis table
    wconn.execute("DELETE FROM synthesis")
    wconn.execute(
        """INSERT INTO synthesis (id, at_a_glance, usage_narrative, top_wins, top_friction,
           claude_md_additions, fun_headline, workflow_prompts, features_to_try)
           VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            json.dumps(parsed.get("at_a_glance", {})),
            parsed.get("usage_narrative", ""),
            json.dumps(parsed.get("top_wins", [])),
            json.dumps(parsed.get("top_friction", [])),  # now includes user_quote per item
            json.dumps(parsed.get("claude_md_additions", [])),
            parsed.get("fun_headline", ""),
            json.dumps(parsed.get("workflow_prompts", [])),
            json.dumps(parsed.get("features_to_try", [])),
        ],
    )
    wconn.commit()

    # Write LLM-generated skill dimension nudges back to skill_nudges table
    dim_nudges = parsed.get("skill_dimension_nudges", {})
    if dim_nudges:
        _update_skill_nudges_from_synthesis(wconn, dim_nudges)

    print("  Synthesis generated successfully.")


def auto_apply_claude_md_suggestions() -> int:
    """Auto-append CLAUDE.md suggestions to each project's CLAUDE.md file.

    Collects per-session suggestions, groups by project cwd, deduplicates,
    and appends new rules under a '## Claude Retro Suggestions' section.

    Returns the number of projects updated.
    """

    conn = get_conn()

    # Get synthesis-level suggestions
    synth_rules = []
    synth_row = conn.execute(
        "SELECT claude_md_additions FROM synthesis WHERE id = 1"
    ).fetchone()
    if synth_row and synth_row[0]:
        try:
            additions = json.loads(synth_row[0]) if isinstance(synth_row[0], str) else synth_row[0]
            for a in additions:
                rule = a.get("rule", "").strip()
                if rule:
                    synth_rules.append(rule)
        except (json.JSONDecodeError, ValueError):
            pass

    # Get per-session suggestions grouped by project cwd
    rows = conn.execute("""
        SELECT DISTINCT r.cwd, j.claude_md_suggestion
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        JOIN raw_entries r ON s.session_id = r.session_id
        WHERE j.claude_md_suggestion IS NOT NULL
          AND j.claude_md_suggestion != ''
          AND r.cwd IS NOT NULL AND r.cwd != ''
        GROUP BY r.cwd, j.claude_md_suggestion
    """).fetchall()

    # Group by cwd
    project_rules: dict[str, list[str]] = {}
    for cwd, rule in rows:
        rule = rule.strip()
        if rule:
            project_rules.setdefault(cwd, []).append(rule)

    # Find distinct project cwds (even if no per-session rules, apply synthesis rules)
    all_cwds = conn.execute("""
        SELECT DISTINCT r.cwd
        FROM raw_entries r
        JOIN sessions s ON r.session_id = s.session_id
        WHERE r.cwd IS NOT NULL AND r.cwd != ''
          AND s.turn_count >= 1
    """).fetchall()

    # Build full rule set per project: synthesis rules + session-specific rules
    updated = 0
    for (cwd,) in all_cwds:
        cwd_path = Path(cwd)
        if not cwd_path.is_dir():
            continue

        # Find the git root (walk up)
        project_root = _find_project_root(cwd_path)
        if project_root is None:
            continue

        all_rules = list(synth_rules)  # synthesis rules apply to all projects
        all_rules.extend(project_rules.get(cwd, []))

        if not all_rules:
            continue

        claude_md = project_root / "CLAUDE.md"
        if _append_rules_to_claude_md(claude_md, all_rules):
            updated += 1

    return updated


def _find_project_root(cwd: Path) -> Path | None:
    """Walk up from cwd to find a project root (has .git or is a code directory)."""
    current = cwd
    home = Path.home()
    while current != current.parent and current != home:
        if (current / ".git").exists():
            return current
        current = current.parent
    # If we hit home without finding .git, use the original cwd if it looks like a project
    if (cwd / ".git").exists() or any(cwd.glob("*.py")) or any(cwd.glob("*.ts")):
        return cwd
    return None


_RETRO_SECTION_HEADER = "## Claude Retro Suggestions"
_RETRO_SECTION_MARKER = "<!-- claude-retro-auto -->"


def _append_rules_to_claude_md(claude_md: Path, rules: list[str]) -> bool:
    """Append rules to a CLAUDE.md file, deduplicating against existing content.

    Returns True if the file was modified.
    """
    # Read existing content
    existing_content = ""
    if claude_md.exists():
        existing_content = claude_md.read_text()

    # Normalize rules: ensure each starts with "- "
    normalized = []
    for rule in rules:
        rule = rule.strip()
        if not rule.startswith("- "):
            rule = "- " + rule
        normalized.append(rule)

    # Extract existing rules from our managed section (if it exists)
    existing_retro_rules = []
    pattern = re.escape(_RETRO_SECTION_MARKER) + r"\n(.*?)\n" + re.escape(_RETRO_SECTION_MARKER)
    match = re.search(pattern, existing_content, re.DOTALL)
    if match:
        existing_retro_rules = [
            line.strip() for line in match.group(1).split("\n") if line.strip()
        ]

    # Content outside our section (for dedup against manually-written rules)
    content_outside_section = existing_content
    if match:
        content_outside_section = existing_content[:match.start()] + existing_content[match.end():]
    outside_lower = content_outside_section.lower()

    # Merge: start with existing retro rules, add new ones that aren't duplicates
    merged = list(existing_retro_rules)
    merged_lower = {r.lstrip("- ").strip().lower()[:60] for r in merged}

    new_count = 0
    for rule in normalized:
        core = rule.lstrip("- ").strip().lower()
        # Skip if already in our section or elsewhere in the file
        if core[:60] in merged_lower or core[:60] in outside_lower:
            continue
        merged.append(rule)
        merged_lower.add(core[:60])
        new_count += 1

    if new_count == 0:
        return False  # Nothing new to add

    section_content = "\n".join(merged)

    if _RETRO_SECTION_MARKER in existing_content:
        # Replace existing section with merged content
        replacement = f"{_RETRO_SECTION_MARKER}\n{section_content}\n{_RETRO_SECTION_MARKER}"
        new_content = re.sub(pattern, replacement, existing_content, flags=re.DOTALL)
    else:
        # Append new section
        separator = "\n\n" if existing_content and not existing_content.endswith("\n\n") else "\n" if existing_content and not existing_content.endswith("\n") else ""
        new_content = existing_content + f"{separator}\n{_RETRO_SECTION_HEADER}\n{_RETRO_SECTION_MARKER}\n{section_content}\n{_RETRO_SECTION_MARKER}\n"

    claude_md.write_text(new_content)
    print(f"    Updated {claude_md} (+{new_count} new, {len(merged)} total rules)")
    return True
