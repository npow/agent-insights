"""Flask REST API."""

import json
import sys
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, Response

from .config import CLAUDE_PROJECTS_DIR
from .db import get_conn, get_writer
from .version import get_version_info
from .export import generate_export_html

if getattr(sys, "frozen", False):
    _static = str(Path(sys._MEIPASS) / "static")
else:
    _static = str(Path(__file__).parent / "static")

app = Flask(__name__, static_folder=_static)

# Ensure schema exists at import time so the app works even when started
# via `flask run` rather than `python -m claude_retro`.
get_writer()

# Set by app.py / __main__.py so /api/status can read worker state
_worker = None


def set_worker(worker):
    global _worker
    _worker = worker


# Priority-ordered patterns; first match wins, minimising "Other"
_FRICTION_PATTERNS = [
    (
        "Ignored User Instruction",
        [
            "contrary to",
            "contrary to user",
            "despite user",
            "despite being told",
            "despite the user",
            "user explicitly",
            "user had explicitly",
            "user said not to",
            "user said they",
            "user asked to remove",
            "user had told",
            "user confirmed",
            "user clarified",
            "user preferred",
            "user specified",
            "user corrected",
            "user redirected",
            "user interrupted",
            "user had to point out",
            "when user told",
            "when user said",
            "after user said",
            "after the user",
            "user wanted",
            "user requested",
            "continuing after",
            "still working on",
            "questioning whether",
            "as user requested",
            "as directed",
            "delegated",
            "instead of doing it himself",
            "suggested user take action",
            "asked user to",
            "user had already clarified",
            "user had clarified",
            "user expected",
            "user's expectation",
            "user stated:",
            "user said:",
            "user said ",
            "user pushed back",
            "user had to",
            "were supposed to be",
            "implementing something different",
        ],
    ),
    (
        "Repeated Failure",
        [
            "consecutive turns",
            "consecutive failed",
            "multiple consecutive",
            "consecutive",
            "cycle of failed",
            "without changing approach",
            "multiple fail",
            "multiple attempt",
            "repeated attempt",
            "required retry",
            "requiring recovery",
            "tried the same",
            "same approach again",
            "still failing after",
            "still not working after",
            "across multiple turns",
            "multiple additional iteration",
            "required multiple",
            "for 26 turns",
            "still insisted",
            "circular debugging",
            "repeated error",
            "repeated the same",
            "going in circles",
            "kept trying",
            "spent 52 turns",
            "spent 100+",
        ],
    ),
    (
        "Misunderstood Request",
        [
            "misunderstood",
            "misinterpreted",
            "confusion about",
            "confused about",
            "confused the",
            "not understanding",
            "not understand",
            "confused why",
            "scope mismatch",
            "misunderstanding that",
            "misunderstand",
            "expectation mismatch",
            "mismatch; original",
            "user's actual concern",
            "user actually wanted",
            "user's actual need",
            "actual need was",
            "rather than brainstorming",
            "conflated",
            "interpreted this as",
            "user pivoted",
        ],
    ),
    (
        "Jumped Ahead Without Clarifying",
        [
            "without asking clarifying",
            "without first asking",
            "without clarifying",
            "without first understanding",
            "should have asked",
            "should have clarified",
            "should have requested",
            "should have first",
            "should have researched",
            "should have figured",
            "before asking",
            "before clarifying",
            "before understanding the core",
            "before understanding",
            "jumped directly into",
            "jumped directly to",
            "jumped to",
            "dove into",
            "without establishing",
            "without validating",
            "without first validating",
            "without explaining",
            "without explaining approach",
            "proceeded without",
            "proceeded before",
            "without reading",
            "without understanding",
            "without requesting",
            "rather than first",
            "launched into",
            "without profiling",
            "before root caus",
            "before testing",
            "without first explaining",
            "without explicitly",
            "should have used",
            "before applying",
            "rather than examining",
            "should have prompted",
            "should have been",
            "without clear justification",
        ],
    ),
    (
        "Wrong Approach",
        [
            "wrong approach",
            "wrong method",
            "wrong strategy",
            "wrong testing",
            "wrong command",
            "wrong tool",
            "wrong direction",
            "wrong installation",
            "wrong import",
            "wrong source",
            "wrong location",
            "wrong path",
            "attempted wrong",
            "proposed mock",
            "without properly root caus",
            "without root caus",
            "incorrect approach",
            "instead of",
            "wrong ",
            "entered plan mode",
            "pursuing strategy",
            "unnecessary complex",
            "slower approach",
            "when direct",
            "requires ci modification",
            "to symlink",
            "symlink approach",
            "over-engineer",
            "veered into",
            "veered away",
            "scope creep",
            "too cautious",
            "too conservative",
            "hesitant to proceed",
            "inappropriate for",
            "is inappropriate",
            "beyond scope",
            "ci modification",
            "architectural mismatch",
            "diverged to",
            "led to regressions",
            "rather than integrated",
            "unexpected line",
            "rather than scoped",
            "rather than examining actual",
        ],
    ),
    (
        "Premature Completion",
        [
            "claimed thorough",
            "claimed everything works",
            "claimed local testing",
            "claimed no issues",
            "marked all tasks done",
            "without actually testing",
            "without actually running",
            "without end-to-end",
            "without running tests",
            "false impression of",
            "comprehensive enough",
            "claimed complete",
            "without verifying in",
            "without fully testing",
        ],
    ),
    (
        "Incomplete Work",
        [
            "never delivered",
            "never synthesized",
            "never answered",
            "left placeholder",
            "left todos",
            "still todos",
            "abruptly with no",
            "ended abruptly",
            "without completing",
            "without delivering",
            "session ended abruptly",
            "without any response",
            "left incomplete",
            "left unimplemented",
            "left outstanding",
            "never completed",
            "no output after",
            "appeared blocked",
            "no visible progress",
            "produced no output",
            "final turns diverged",
        ],
    ),
    (
        "Missed Issues",
        [
            "didn't",
            "did not",
            "failed to",
            "forgot to",
            "overlooked",
            "without identifying",
            "without checking",
            "without verifying",
            "not check",
            "not identify",
            "not recogni",
            "incomplete",
            "missed the",
            "missed that",
            "missed a",
            "not converting",
            "not installing",
            "not putting",
            "not finding",
            "not debugging",
            "unable to",
            "not able to",
            "not looking at",
            "needed user to provide",
            "not tracking",
            "not using correct",
            "weren't caught",
            "not caught",
            "not proactively",
            "hadn't anticipated",
            "not comprehensive",
            "without running",
            "wasn't caught",
            "without proactively",
            "lacked context",
            "no automated",
        ],
    ),
    (
        "Buggy Code",
        [
            "incorrect",
            "incorrectly",
            "duplicate",
            "would incorrectly",
            "logic error",
            "wrong output",
            "wrong result",
            "but this was",
            "causing attri",
            "causing type",
            "causing test",
            "causing import",
            "hardcoded",
            "hard-coded",
            "committed unwanted",
            "commented out critical",
            "too implementation-dependent",
            "deployed without adequate",
            "caused segmentation",
            "caused typeerror",
            "caused crash",
            "still inconsistent",
            "functionality broke",
            "hardcoding",
            "user caught this",
            "was empty",
        ],
    ),
    (
        "Wrong Assumption",
        [
            "assumed ",
            "incorrect assumption",
            "assumption about",
            "stated that",
            "claimed that",
            "confidently stated",
            "asserted that",
            "second-guess",
            "misunderstanding that",
            "claimed no",
            "false impression",
            "speculated",
            "assuming it was",
            "user questioned whether",
        ],
    ),
    (
        "User Rejected Action",
        [
            "user rejected",
            "user cancelled",
            "user denied",
            "refused",
            "permission denied",
            "rejected by user as",
        ],
    ),
    (
        "Tool/Bash Error",
        [
            "bash error",
            "bash command error",
            "tool error",
            "command error",
            "read tool error",
            "edit error",
            "git operation failed",
            "commit commands failed",
            "git commit failed",
            "hit minio",
            "connection refused",
            "timeout",
            "webfetch",
            "fetch error",
            "requiring vpn",
            "requiring authentication",
            "needed to be killed",
            "errored out",
            "tool failed",
            "connection issues",
            "mcp had",
            "failed with error",
            "which errored",
            "execution errors",
            "bash access to",
        ],
    ),
]


@app.route("/api/status")
def api_status():
    if _worker is None:
        return jsonify({"state": "idle", "step": "", "ready": True})
    return jsonify(_worker.status)


def _row_to_dict(row, columns):
    return {col: _serialize(val) for col, val in zip(columns, row)}


def _serialize(val):
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/version")
def api_version():
    return jsonify(get_version_info())


@app.route("/api/export")
def api_export():
    """Export verdict and prescriptions as standalone HTML."""
    from datetime import datetime

    html = generate_export_html()
    filename = f"claude-retro-{datetime.now().strftime('%Y%m%d')}.html"

    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/overview")
def api_overview():
    conn = get_conn()

    _filter = """
        WHERE turn_count >= 1
          AND first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    stats = conn.execute(f"""
        SELECT
            COUNT(*) as total_sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as total_hours,
            COUNT(DISTINCT project_name) as total_projects,
            AVG(turn_count) as avg_turns,
            SUM(user_prompt_count + assistant_msg_count) as total_messages,
            COUNT(DISTINCT DATE(started_at)) as active_days,
            SUM(s.tool_use_count) as total_tool_calls,
            COALESCE(SUM(f.total_input_tokens), 0) as total_input_tokens,
            COALESCE(SUM(f.total_output_tokens), 0) as total_output_tokens
        FROM sessions s
        LEFT JOIN session_features f ON s.session_id = f.session_id
        {_filter}
    """).fetchone()

    # Median and p90 of messages per session
    msg_counts = conn.execute(f"""
        SELECT user_prompt_count + assistant_msg_count as msgs
        FROM sessions
        {_filter}
        ORDER BY msgs
    """).fetchall()
    msg_list = [r[0] for r in msg_counts if r[0] is not None]
    if msg_list:
        median_msgs = msg_list[len(msg_list) // 2]
        p90_msgs = msg_list[int(len(msg_list) * 0.9)]
        avg_msgs = round(sum(msg_list) / len(msg_list), 1)
    else:
        median_msgs = p90_msgs = avg_msgs = 0

    # Top project concentration
    top_proj = conn.execute(f"""
        SELECT project_name, COUNT(*) as cnt
        FROM sessions
        {_filter}
        GROUP BY project_name
        ORDER BY cnt DESC
        LIMIT 1
    """).fetchone()

    trajectory_dist = conn.execute(f"""
        SELECT trajectory, COUNT(*) as count
        FROM sessions
        {_filter}
        GROUP BY trajectory
        ORDER BY count DESC
    """).fetchall()

    cursor = conn.execute("""
        SELECT * FROM baselines ORDER BY window_size
    """)
    baselines = cursor.fetchall()
    baseline_cols = [d[0] for d in cursor.description]

    total_sessions = stats[0] or 0

    return jsonify(
        {
            "total_sessions": total_sessions,
            "avg_convergence": round(stats[1] or 0, 3),
            "avg_drift": round(stats[2] or 0, 3),
            "avg_thrash": round(stats[3] or 0, 3),
            "total_hours": round(stats[4] or 0, 1),
            "total_projects": stats[5],
            "avg_turns": round(stats[6] or 0, 1),
            "total_messages": stats[7] or 0,
            "active_days": stats[8] or 0,
            "msgs_per_session_avg": avg_msgs,
            "msgs_per_session_median": median_msgs,
            "msgs_per_session_p90": p90_msgs,
            "top_project": top_proj[0] if top_proj else None,
            "top_project_pct": round(top_proj[1] / total_sessions, 2)
            if top_proj and total_sessions
            else 0,
            "trajectory_distribution": {t: c for t, c in trajectory_dist},
            "baselines": [_row_to_dict(b, baseline_cols) for b in baselines],
            "total_tool_calls": int(stats[9] or 0),
            "total_input_tokens": int(stats[10] or 0),
            "total_output_tokens": int(stats[11] or 0),
            # Estimated cost: Sonnet 3.5/3.7 pricing ($3/MTok in, $15/MTok out)
            "estimated_cost_usd": round(
                (stats[10] or 0) / 1_000_000 * 3.0
                + (stats[11] or 0) / 1_000_000 * 15.0,
                2,
            ),
        }
    )


@app.route("/api/sessions")
def api_sessions():
    conn = get_conn()
    project = request.args.get("project")
    intent = request.args.get("intent")
    trajectory = request.args.get("trajectory")
    search = request.args.get("search")
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    sort = request.args.get("sort", "started_at DESC")

    # Whitelist sort columns
    allowed_sorts = {
        "started_at": "s.started_at",
        "convergence": "s.convergence_score",
        "drift": "s.drift_score",
        "thrash": "s.thrash_score",
        "duration": "s.duration_seconds",
        "turns": "s.turn_count",
        "misalignments": "COALESCE(j.misalignment_count, 0)",
        "productivity": "COALESCE(j.productivity_ratio, 0)",
    }
    sort_parts = sort.split()
    sort_col = allowed_sorts.get(sort_parts[0], "s.started_at")
    sort_dir = (
        "DESC" if len(sort_parts) < 2 or sort_parts[1].upper() == "DESC" else "ASC"
    )

    conditions = [
        "s.turn_count >= 1",
        "s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'",
    ]
    params = []

    if project:
        conditions.append("s.project_name = ?")
        params.append(project)
    if intent:
        conditions.append("s.intent = ?")
        params.append(intent)
    if trajectory:
        conditions.append("s.trajectory = ?")
        params.append(trajectory)
    if search:
        conditions.append("(s.first_prompt LIKE ? OR s.session_id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = "WHERE " + " AND ".join(conditions)

    total = conn.execute(f"SELECT COUNT(*) FROM sessions s {where}", params).fetchone()[
        0
    ]

    rows = conn.execute(
        f"""
        SELECT s.session_id, s.project_name, s.started_at, s.ended_at, s.duration_seconds,
               s.user_prompt_count, s.assistant_msg_count, s.tool_use_count, s.tool_error_count,
               s.turn_count, s.first_prompt, s.intent, s.trajectory,
               s.convergence_score, s.drift_score, s.thrash_score,
               j.outcome, j.misalignment_count, j.productivity_ratio
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        {where}
        ORDER BY {sort_col} {sort_dir}
        LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()

    cols = [
        "session_id",
        "project_name",
        "started_at",
        "ended_at",
        "duration_seconds",
        "user_prompt_count",
        "assistant_msg_count",
        "tool_use_count",
        "tool_error_count",
        "turn_count",
        "first_prompt",
        "intent",
        "trajectory",
        "convergence_score",
        "drift_score",
        "thrash_score",
        "judgment_outcome",
        "misalignment_count",
        "productivity_ratio",
    ]

    return jsonify(
        {
            "total": total,
            "sessions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/sessions/<session_id>")
def api_session_detail(session_id):
    conn = get_conn()

    cursor = conn.execute(
        """
        SELECT * FROM sessions WHERE session_id = ?
    """,
        [session_id],
    )
    session = cursor.fetchone()

    if not session:
        return jsonify({"error": "Session not found"}), 404

    session_cols = [d[0] for d in cursor.description]

    cursor2 = conn.execute(
        """
        SELECT * FROM session_features WHERE session_id = ?
    """,
        [session_id],
    )
    features = cursor2.fetchone()
    feature_cols = [d[0] for d in cursor2.description] if features else []

    tools = conn.execute(
        """
        SELECT tool_name, use_count, error_count,
               COALESCE(total_duration_ms, 0), COALESCE(avg_duration_ms, 0)
        FROM session_tool_usage WHERE session_id = ?
        ORDER BY use_count DESC
    """,
        [session_id],
    ).fetchall()

    cursor3 = conn.execute(
        """
        SELECT * FROM session_judgments WHERE session_id = ?
    """,
        [session_id],
    )
    judgment = cursor3.fetchone()
    judgment_cols = [d[0] for d in cursor3.description] if judgment else []

    result = {
        "session": _row_to_dict(session, session_cols),
        "features": _row_to_dict(features, feature_cols) if features else {},
        "tools": [
            {
                "tool_name": t[0],
                "use_count": t[1],
                "error_count": t[2],
                "total_duration_ms": t[3],
                "avg_duration_ms": round(t[4]) if t[4] else 0,
            }
            for t in tools
        ],
    }
    if judgment:
        jd = _row_to_dict(judgment, judgment_cols)
        # Parse JSON string fields for the frontend
        for field in (
            "prompt_missing",
            "underspecified_parts",
            "misalignments",
            "corrections",
            "waste_breakdown",
            "friction_categories",
        ):
            if jd.get(field) and isinstance(jd[field], str):
                try:
                    jd[field] = json.loads(jd[field])
                except (json.JSONDecodeError, ValueError):
                    pass
        result["judgment"] = jd
        # Include narrative fields at top level for easy access
        result["narrative"] = {
            "narrative": jd.get("narrative"),
            "what_worked": jd.get("what_worked"),
            "what_failed": jd.get("what_failed"),
            "user_quote": jd.get("user_quote"),
            "claude_md_suggestion": jd.get("claude_md_suggestion"),
            "claude_md_rationale": jd.get("claude_md_rationale"),
        }
    else:
        result["judgment"] = None
        result["narrative"] = None

    return jsonify(result)


@app.route("/api/sessions/<session_id>/judgment")
def api_session_judgment(session_id):
    conn = get_conn()
    cursor = conn.execute(
        "SELECT * FROM session_judgments WHERE session_id = ?", [session_id]
    )
    judgment = cursor.fetchone()
    if not judgment:
        return jsonify({"error": "No judgment found"}), 404
    cols = [d[0] for d in cursor.description]
    jd = _row_to_dict(judgment, cols)
    for field in (
        "prompt_missing",
        "underspecified_parts",
        "misalignments",
        "corrections",
        "waste_breakdown",
        "friction_categories",
    ):
        if jd.get(field) and isinstance(jd[field], str):
            try:
                jd[field] = json.loads(jd[field])
            except (json.JSONDecodeError, ValueError):
                pass
    return jsonify(jd)


@app.route("/api/sessions/<session_id>/timeline")
def api_session_timeline(session_id):
    conn = get_conn()

    full = request.args.get("full", "0") == "1"
    text_col = "user_text" if full else "SUBSTR(user_text, 1, 200)"
    content_col = "text_content" if full else "SUBSTR(text_content, 1, 200)"

    entries = conn.execute(
        f"""
        SELECT entry_id, entry_type, timestamp_utc, user_text_length,
               text_length, tool_names, is_tool_result, tool_result_error,
               system_subtype, duration_ms,
               CASE WHEN user_text_length > 0 THEN {text_col} ELSE {content_col} END as preview,
               CASE WHEN user_text_length > 0 THEN {text_col} ELSE NULL END as user_text
        FROM raw_entries
        WHERE session_id = ? AND NOT is_sidechain
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    cols = [
        "entry_id",
        "entry_type",
        "timestamp_utc",
        "user_text_length",
        "text_length",
        "tool_names",
        "is_tool_result",
        "tool_result_error",
        "system_subtype",
        "duration_ms",
        "preview",
        "user_text",
    ]

    return jsonify(
        {
            "timeline": [_row_to_dict(e, cols) for e in entries],
        }
    )


@app.route("/api/sessions/<session_id>/rich-timeline")
def api_session_rich_timeline(session_id):
    """Read JSONL directly to return full tool inputs + result content."""
    conn = get_conn()

    row = conn.execute(
        "SELECT project_name FROM sessions WHERE session_id = ?", [session_id]
    ).fetchone()
    if not row:
        return jsonify({"error": "Session not found", "timeline": []}), 404

    project_name = row[0]
    jsonl_path = CLAUDE_PROJECTS_DIR / project_name / f"{session_id}.jsonl"

    if not jsonl_path.exists():
        return jsonify({"error": "JSONL not found", "timeline": []}), 404

    MAX_TEXT = 400  # assistant text / tool inputs
    MAX_RESULT = 100_000  # tool results — send full content, UI handles collapse
    turns = []

    with open(jsonl_path, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue

            entry_type = d.get("type")
            if entry_type not in ("user", "assistant", "system"):
                continue
            if d.get("isSidechain"):
                continue

            msg = d.get("message", {})
            content = msg.get("content", "")

            turn = {
                "type": entry_type,
                "timestamp": d.get("timestamp", ""),
                "text": "",
                "tools": [],
                "is_tool_result": False,
                "is_error": False,
                "tool_id": None,
                "result_preview": None,
                "system_subtype": d.get("subtype"),
                "duration_ms": d.get("durationMs", 0),
            }

            if isinstance(content, str):
                turn["text"] = content[:MAX_TEXT]
            elif isinstance(content, list):
                text_parts = []
                for block in content:
                    btype = block.get("type", "")
                    if btype == "text":
                        text_parts.append(block.get("text", ""))
                    elif btype == "tool_use":
                        inp = block.get("input", {})
                        inp_str = json.dumps(inp, ensure_ascii=False)
                        turn["tools"].append(
                            {
                                "name": block.get("name", ""),
                                "id": block.get("id", ""),
                                "input_preview": inp_str[:MAX_TEXT],
                            }
                        )
                    elif btype == "tool_result":
                        turn["is_tool_result"] = True
                        turn["is_error"] = bool(block.get("is_error", False))
                        turn["tool_id"] = block.get("tool_use_id", "")
                        rc = block.get("content", "")
                        if isinstance(rc, list):
                            rc = " ".join(
                                b.get("text", "") for b in rc if b.get("type") == "text"
                            )
                        turn["result_preview"] = str(rc)[:MAX_RESULT] if rc else None
                if text_parts:
                    turn["text"] = "\n".join(text_parts)[:MAX_TEXT]

            turns.append(turn)

    return jsonify({"timeline": turns})


@app.route("/api/intents")
def api_intents():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            intent,
            COUNT(*) as count,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            AVG(duration_seconds) as avg_duration,
            AVG(turn_count) as avg_turns
        FROM sessions
        GROUP BY intent
        ORDER BY count DESC
    """).fetchall()

    cols = [
        "intent",
        "count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "avg_duration",
        "avg_turns",
    ]

    return jsonify(
        {
            "intents": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/trends")
def api_trends():
    conn = get_conn()
    days = int(request.args.get("days", 30))

    rows = conn.execute(
        """
        SELECT
            DATE(started_at) as day,
            COUNT(*) as sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as hours
        FROM sessions
        WHERE started_at >= DATE('now', '-? days')
        GROUP BY DATE(started_at)
        ORDER BY day
    """.replace("?", str(int(days)))
    ).fetchall()

    cols = ["day", "sessions", "avg_convergence", "avg_drift", "avg_thrash", "hours"]

    return jsonify(
        {
            "trends": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/search")
def api_search():
    """Full-text search across all messages using FTS5."""
    conn = get_conn()
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 30)), 100)
    project = request.args.get("project")

    if not q or len(q) < 2:
        return jsonify({"results": [], "query": q})

    # Escape FTS5 special characters and wrap in quotes for phrase matching
    fts_query = q.replace('"', '""')
    if " " in fts_query:
        fts_query = f'"{fts_query}"'

    try:
        params = [fts_query]
        project_filter = ""
        if project:
            project_filter = "AND s.project_name = ?"
            params.append(project)
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT
                messages_fts.session_id,
                messages_fts.entry_type,
                s.project_name,
                s.first_prompt,
                s.started_at,
                snippet(messages_fts, 0, '<mark>', '</mark>', '...', 40) as snippet,
                s.started_at as timestamp_utc
            FROM messages_fts
            JOIN sessions s ON messages_fts.session_id = s.session_id
            WHERE messages_fts MATCH ?
              {project_filter}
            ORDER BY rank
            LIMIT ?
        """,
            params,
        ).fetchall()
    except Exception:
        # FTS query failed — fall back to LIKE search
        like_q = f"%{q}%"
        params = [like_q, like_q]
        project_filter = ""
        if project:
            project_filter = "AND s.project_name = ?"
            params.append(project)
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT
                r.session_id,
                r.entry_type,
                s.project_name,
                s.first_prompt,
                s.started_at,
                SUBSTR(COALESCE(r.user_text, r.text_content, ''), 1, 200) as snippet,
                r.timestamp_utc
            FROM raw_entries r
            JOIN sessions s ON r.session_id = s.session_id
            WHERE (r.user_text LIKE ? OR r.text_content LIKE ?)
              {project_filter}
            ORDER BY r.timestamp_utc DESC
            LIMIT ?
        """,
            params,
        ).fetchall()

    results = []
    for row in rows:
        sid = row[0]
        # Deduplicate by session (show max 2 results per session)
        count = sum(1 for r in results if r["session_id"] == sid)
        if count >= 2:
            continue
        results.append(
            {
                "session_id": sid,
                "entry_type": row[1],
                "project": row[2],
                "first_prompt": (row[3] or "")[:80],
                "started_at": _serialize(row[4]),
                "snippet": row[5],
                "timestamp": _serialize(row[6]),
            }
        )

    return jsonify({"results": results, "query": q})


@app.route("/api/actions")
def api_actions():
    from .prescriptions import generate_actions

    actions = generate_actions()
    return jsonify({"actions": actions})


@app.route("/api/prescriptions")
def api_prescriptions():
    conn = get_conn()

    rows = conn.execute("""
        SELECT id, category, title, description, evidence, confidence, dismissed, created_at
        FROM prescriptions
        WHERE dismissed = FALSE
        ORDER BY confidence DESC
    """).fetchall()

    cols = [
        "id",
        "category",
        "title",
        "description",
        "evidence",
        "confidence",
        "dismissed",
        "created_at",
    ]

    return jsonify(
        {
            "prescriptions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/prescriptions/<int:pid>/dismiss", methods=["POST"])
def api_dismiss_prescription(pid):
    conn = get_conn()
    conn.execute("UPDATE prescriptions SET dismissed = TRUE WHERE id = ?", [pid])
    return jsonify({"ok": True})


@app.route("/api/tools")
def api_tools():
    conn = get_conn()

    rows = conn.execute("""
        SELECT tool_name, SUM(use_count) as total_uses, SUM(error_count) as total_errors
        FROM session_tool_usage
        GROUP BY tool_name
        ORDER BY total_uses DESC
    """).fetchall()

    return jsonify(
        {
            "tools": [
                {"tool_name": r[0], "total_uses": r[1], "total_errors": r[2]}
                for r in rows
            ],
        }
    )


@app.route("/api/projects")
def api_projects():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            s.project_name,
            COUNT(*) as session_count,
            AVG(s.convergence_score) as avg_convergence,
            AVG(s.drift_score) as avg_drift,
            AVG(s.thrash_score) as avg_thrash,
            SUM(s.duration_seconds) / 3600.0 as total_hours,
            MAX(s.started_at) as last_active,
            SUM(s.tool_error_count) as total_errors,
            COALESCE(SUM(f.total_input_tokens), 0) as total_input_tokens,
            COALESCE(SUM(f.total_output_tokens), 0) as total_output_tokens,
            AVG(j.productivity_ratio) as avg_productivity,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN j.outcome IS NOT NULL THEN 1 ELSE 0 END), 0) as completion_rate,
            AVG(j.misalignment_count) as avg_misalignments
        FROM sessions s
        LEFT JOIN session_features f ON s.session_id = f.session_id
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%%'
        GROUP BY s.project_name
        ORDER BY session_count DESC
    """).fetchall()

    cols = [
        "project_name",
        "session_count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "total_hours",
        "last_active",
        "total_errors",
        "total_input_tokens",
        "total_output_tokens",
        "avg_productivity",
        "completion_rate",
        "avg_misalignments",
    ]

    return jsonify(
        {
            "projects": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a full refresh (ingest + LLM judge) in the background.

    Non-blocking — returns immediately. Poll /api/status for progress.
    Accepts optional JSON body: {"concurrency": 12}
    """
    if _worker is None:
        return jsonify({"error": "No background worker available"}), 500

    if _worker.is_busy:
        # Queue it — the worker will pick it up after the current run finishes
        _worker.request_refresh(
            concurrency=max(
                1,
                min(
                    32,
                    int((request.get_json(silent=True) or {}).get("concurrency", 12)),
                ),
            )
        )
        return jsonify({"ok": True, "queued": True, "concurrency": 12})

    body = request.get_json(silent=True) or {}
    concurrency = body.get("concurrency", 12)
    concurrency = max(1, min(32, int(concurrency)))

    _worker.request_refresh(concurrency=concurrency)
    return jsonify({"ok": True, "concurrency": concurrency})


@app.route("/api/fill-narratives", methods=["POST"])
def api_fill_narratives():
    """Re-judge sessions that are missing narrative text (non-blocking)."""
    if _worker is None:
        return jsonify({"error": "No background worker available"}), 500
    if _worker.is_busy:
        return jsonify(
            {"ok": False, "message": "Worker is busy — try again later"}
        ), 409
    conn = get_conn()
    missing = conn.execute("""
        SELECT COUNT(*) FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE (j.narrative IS NULL OR j.narrative = '') AND s.turn_count >= 1
    """).fetchone()[0]
    if missing == 0:
        return jsonify(
            {"ok": True, "message": "All sessions already have narratives", "count": 0}
        )
    body = request.get_json(silent=True) or {}
    concurrency = max(1, min(32, int(body.get("concurrency", 12))))
    _worker.request_fill_narratives(concurrency=concurrency)
    return jsonify({"ok": True, "count": missing, "concurrency": concurrency})


@app.route("/api/judgments/stats")
def api_judgment_stats():
    conn = get_conn()

    # Only count judgments for meaningful sessions (same filter as overview/session list)
    _jfilter = """
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    total = conn.execute(f"SELECT COUNT(*) {_jfilter}").fetchone()[0]
    if total == 0:
        return jsonify({"total_judged": 0})

    outcome_dist = conn.execute(f"""
        SELECT j.outcome, COUNT(*) as count
        {_jfilter}
        GROUP BY j.outcome ORDER BY count DESC
    """).fetchall()

    avgs = conn.execute(f"""
        SELECT
            AVG(j.prompt_clarity) as avg_clarity,
            AVG(j.prompt_completeness) as avg_completeness,
            AVG(j.productivity_ratio) as avg_productivity,
            AVG(j.misalignment_count) as avg_misalignments,
            SUM(CASE WHEN j.misalignment_count > 0 THEN 1 ELSE 0 END) as sessions_with_misalignment,
            SUM(CASE WHEN j.narrative IS NOT NULL AND j.narrative != '' THEN 1 ELSE 0 END) as narrative_count
        {_jfilter}
    """).fetchone()

    return jsonify(
        {
            "total_judged": total,
            "outcome_distribution": {r[0]: r[1] for r in outcome_dist},
            "avg_clarity": round(avgs[0] or 0, 3),
            "avg_completeness": round(avgs[1] or 0, 3),
            "avg_productivity": round(avgs[2] or 0, 3),
            "avg_misalignments": round(avgs[3] or 0, 2),
            "misalignment_rate": round((avgs[4] or 0) / total, 3) if total else 0,
            "narrative_count": avgs[5] or 0,
        }
    )


@app.route("/api/patterns")
def api_patterns():
    conn = get_conn()

    # --- Prompt gap clustering ---
    gap_rows = conn.execute("""
        SELECT prompt_missing FROM session_judgments
        WHERE prompt_missing IS NOT NULL AND prompt_missing != '[]'
    """).fetchall()

    GAP_CATEGORIES = {
        "context": [
            "repo",
            "codebase",
            "file",
            "directory",
            "structure",
            "existing",
            "path",
            "folder",
            "project",
        ],
        "requirements": [
            "expected",
            "behavior",
            "output",
            "format",
            "specific",
            "requirement",
            "result",
            "goal",
        ],
        "constraints": [
            "environment",
            "version",
            "dependency",
            "platform",
            "setup",
            "config",
            "os",
            "runtime",
        ],
        "error_details": [
            "error",
            "message",
            "stack",
            "trace",
            "log",
            "exception",
            "warning",
            "failure",
        ],
        "scope": [
            "which",
            "where",
            "boundary",
            "limit",
            "priority",
            "scope",
            "range",
            "subset",
        ],
    }

    gap_counts = {cat: {"count": 0, "examples": []} for cat in GAP_CATEGORIES}
    total_gap_items = 0
    for (raw,) in gap_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                text = str(item).lower()
                total_gap_items += 1
                matched = False
                for cat, keywords in GAP_CATEGORIES.items():
                    if any(kw in text for kw in keywords):
                        gap_counts[cat]["count"] += 1
                        if len(gap_counts[cat]["examples"]) < 3:
                            gap_counts[cat]["examples"].append(str(item))
                        matched = True
                        break
                if not matched:
                    # Assign to "other" implicitly by not counting
                    pass
        except (json.JSONDecodeError, TypeError):
            continue

    prompt_gaps = sorted(
        [
            {
                "category": cat,
                "examples": info["examples"],
                "count": info["count"],
                "pct": round(info["count"] / total_gap_items, 2)
                if total_gap_items
                else 0,
            }
            for cat, info in gap_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Misalignment theme clustering ---
    mis_rows = conn.execute("""
        SELECT misalignments FROM session_judgments
        WHERE misalignments IS NOT NULL AND misalignments != '[]'
    """).fetchall()

    THEME_KEYWORDS = {
        "tool_overuse": ["tool", "unnecessary", "redundant", "excessive", "repeated"],
        "wrong_approach": [
            "wrong",
            "incorrect",
            "different approach",
            "should have",
            "instead of",
        ],
        "scope_drift": [
            "scope",
            "beyond",
            "unrelated",
            "off-topic",
            "tangent",
            "extra",
        ],
        "format_mismatch": ["format", "style", "convention", "naming", "pattern"],
        "misunderstood_intent": [
            "misunderstood",
            "misinterpret",
            "not what",
            "didn't ask",
            "intent",
        ],
        "ignored_feedback": [
            "ignored",
            "repeated",
            "already said",
            "told you",
            "again",
        ],
    }

    theme_counts = {
        t: {"count": 0, "example": "", "sessions": []} for t in THEME_KEYWORDS
    }
    for (raw,) in mis_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                desc = (
                    item.get("description", str(item))
                    if isinstance(item, dict)
                    else str(item)
                ).lower()
                for theme, keywords in THEME_KEYWORDS.items():
                    if any(kw in desc for kw in keywords):
                        theme_counts[theme]["count"] += 1
                        if not theme_counts[theme]["example"]:
                            theme_counts[theme]["example"] = (
                                item.get("description", str(item))
                                if isinstance(item, dict)
                                else str(item)
                            )
                        break
        except (json.JSONDecodeError, TypeError):
            continue

    misalignment_themes = sorted(
        [
            {
                "theme": theme.replace("_", " "),
                "count": info["count"],
                "example": info["example"],
            }
            for theme, info in theme_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Behavioral correlations ---
    correlations = []

    # Prompt length vs productivity
    prompt_bins = conn.execute("""
        SELECT
            CASE
                WHEN LENGTH(s.first_prompt) < 100 THEN 'short (<100 chars)'
                WHEN LENGTH(s.first_prompt) < 500 THEN 'medium (100-500 chars)'
                ELSE 'long (>500 chars)'
            END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
        ORDER BY avg_prod DESC
    """).fetchall()
    if len(prompt_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in prompt_bins
        ]
        correlations.append(
            {
                "factor": "First prompt length",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Corrections vs completion
    corr_bins = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero corrections' ELSE 'has corrections' END as bin,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_pct,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(corr_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0f}% completion rate, {b[2]:.0%} productivity ({b[3]} sessions)"
            for b in corr_bins
        ]
        correlations.append(
            {
                "factor": "Corrections impact",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Unique tools vs productivity
    tool_bins = conn.execute("""
        SELECT
            CASE WHEN f.unique_tools_used < 5 THEN 'focused (<5 tools)' ELSE 'broad (5+ tools)' END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(tool_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in tool_bins
        ]
        correlations.append(
            {
                "factor": "Tool breadth",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # --- Worst sessions ---
    worst = conn.execute("""
        SELECT j.session_id, j.misalignment_count, j.outcome,
               SUBSTR(s.first_prompt, 1, 120) as prompt_preview
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignment_count > 0
        ORDER BY j.misalignment_count DESC
        LIMIT 5
    """).fetchall()

    worst_sessions = [
        {
            "session_id": w[0],
            "misalignments": w[1],
            "outcome": w[2],
            "prompt_preview": w[3],
        }
        for w in worst
    ]

    return jsonify(
        {
            "prompt_gaps": prompt_gaps,
            "misalignment_themes": misalignment_themes,
            "behavioral_correlations": correlations,
            "worst_sessions": worst_sessions,
        }
    )


@app.route("/api/skills/dimensions")
def api_skill_dimensions():
    from .config import SKILL_DIMENSIONS

    dims = []
    for dim_id in sorted(SKILL_DIMENSIONS.keys(), key=lambda x: int(x[1:])):
        d = SKILL_DIMENSIONS[dim_id]
        dims.append(
            {
                "id": dim_id,
                "name": d["name"],
                "short": d["short"],
                "weight": d["weight"],
                "color": d["color"],
            }
        )
    return jsonify({"dimensions": dims})


@app.route("/api/skills/profile")
def api_skill_profile():
    conn = get_conn()

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return jsonify({"profile": None})

    cols = [d[0] for d in cursor.description]
    p = _row_to_dict(profile, cols)
    return jsonify({"profile": p})


@app.route("/api/skills/session/<session_id>")
def api_skill_session(session_id):
    conn = get_conn()

    cursor = conn.execute(
        "SELECT * FROM session_skills WHERE session_id = ?", [session_id]
    )
    row = cursor.fetchone()
    if not row:
        return jsonify({"skills": None})

    cols = [d[0] for d in cursor.description]
    return jsonify({"skills": _row_to_dict(row, cols)})


@app.route("/api/skills/nudges")
def api_skill_nudges():
    from .config import SKILL_DIMENSIONS

    conn = get_conn()
    rows = conn.execute("""
        SELECT id, dimension, current_level, target_level, nudge_text,
               evidence, frequency, dismissed, created_at
        FROM skill_nudges
        WHERE dismissed = FALSE
        ORDER BY created_at DESC
    """).fetchall()

    cols = [
        "id",
        "dimension",
        "current_level",
        "target_level",
        "nudge_text",
        "evidence",
        "frequency",
        "dismissed",
        "created_at",
    ]

    nudges = []
    for r in rows:
        nd = _row_to_dict(r, cols)
        dim_id = nd.get("dimension", "")
        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        nd["dimension_name"] = dim_info.get("name", dim_id)
        nd["dimension_color"] = dim_info.get("color", "#8b8fa3")
        nudges.append(nd)

    return jsonify({"nudges": nudges})


@app.route("/api/skills/nudges/<int:nid>/dismiss", methods=["POST"])
def api_dismiss_skill_nudge(nid):
    conn = get_conn()
    conn.execute("UPDATE skill_nudges SET dismissed = TRUE WHERE id = ?", [nid])
    return jsonify({"ok": True})


@app.route("/api/skills/dimensions/detail")
def api_skill_dimensions_detail():
    """Return all dimensions with nudge text for next level + example sessions."""
    from .config import SKILL_DIMENSIONS, SKILL_NUDGES

    conn = get_conn()

    # Get profile
    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return jsonify({"dimensions": []})
    cols = [d[0] for d in cursor.description]
    p = _row_to_dict(profile, cols)
    gaps = [p.get("gap_1"), p.get("gap_2"), p.get("gap_3")]

    results = []
    for dim_id in sorted(SKILL_DIMENSIONS.keys(), key=lambda x: int(x[1:])):
        d = SKILL_DIMENSIONS[dim_id]
        num = int(dim_id[1:])
        score = p.get(f"d{num}_score", 0) or 0
        level = int(score)
        is_gap = dim_id in gaps
        target = level + 1

        # Nudge text for next level (works for ALL dimensions, not just gaps)
        nudge = SKILL_NUDGES.get((dim_id, target), "")

        # Find example sessions: best demos (high level) and opportunities (high opp)
        level_col = f"d{num}_level"
        opp_col = f"d{num}_opportunity"
        examples = conn.execute(f"""
            SELECT sk.session_id, sk.{level_col}, sk.{opp_col},
                   s.first_prompt, s.started_at, s.duration_seconds,
                   s.project_name, j.outcome, j.productivity_ratio
            FROM session_skills sk
            JOIN sessions s ON sk.session_id = s.session_id
            LEFT JOIN session_judgments j ON sk.session_id = j.session_id
            WHERE (sk.{level_col} >= 2 OR sk.{opp_col} > sk.{level_col})
              AND s.turn_count >= 1
              AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
            ORDER BY sk.{level_col} DESC, s.started_at DESC
            LIMIT 5
        """).fetchall()

        example_sessions = []
        for sid, lv, opp, prompt, started, dur, project, outcome, prod in examples:
            label = f"L{lv}"
            if opp > lv:
                label += f" (could be L{opp})"
            short_project = (
                (project or "")
                .replace("-Users-npow-code-", "")
                .replace("-Users-npow-", "")
            )
            example_sessions.append(
                {
                    "session_id": sid,
                    "level": lv,
                    "opportunity": opp,
                    "label": label,
                    "first_prompt": (prompt or "")[:80],
                    "started_at": _serialize(started),
                    "duration": dur,
                    "project": short_project,
                    "outcome": outcome,
                    "productivity": prod,
                }
            )

        results.append(
            {
                "id": dim_id,
                "name": d["name"],
                "short": d["short"],
                "color": d["color"],
                "score": round(score, 1),
                "level": level,
                "is_gap": is_gap,
                "next_level": target,
                "nudge": nudge,
                "examples": example_sessions,
            }
        )

    return jsonify({"dimensions": results})


@app.route("/api/synthesis")
def api_synthesis():
    """Return the cross-session synthesis report."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM synthesis WHERE id = 1").fetchone()
    if not row:
        return jsonify({"synthesis": None})

    cols = [
        d[0] for d in conn.execute("SELECT * FROM synthesis WHERE id = 1").description
    ]
    result = _row_to_dict(row, cols)

    # Parse JSON fields
    for field in (
        "at_a_glance",
        "top_wins",
        "top_friction",
        "claude_md_additions",
        "workflow_prompts",
        "features_to_try",
    ):
        if result.get(field) and isinstance(result[field], str):
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, ValueError):
                pass

    return jsonify({"synthesis": result})


@app.route("/api/synthesis/delta")
def api_synthesis_delta():
    """Return delta between current and previous synthesis run."""
    conn = get_conn()

    # Current stats (live from DB)
    cur = conn.execute("""
        SELECT COUNT(*) as sess, AVG(productivity_ratio) as prod,
               AVG(misalignment_count) as mis
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
    """).fetchone()
    cur_sessions = cur[0] or 0
    cur_prod = cur[1] or 0.0
    cur_mis = cur[2] or 0.0

    # Previous run from synthesis_history
    prev = conn.execute("""
        SELECT session_count, productivity_avg, friction_counts, generated_at
        FROM synthesis_history ORDER BY id DESC LIMIT 1
    """).fetchone()

    if not prev:
        return jsonify({"delta": None, "message": "No previous run to compare"})

    prev_sessions = prev[0] or 0
    prev_prod = prev[1] or 0.0
    prev_generated_at = prev[3]

    prev_friction = {}
    if prev[2]:
        try:
            prev_friction = json.loads(prev[2])
        except Exception:
            pass

    conn.execute("""
        SELECT COUNT(*), AVG(misalignment_count)
        FROM session_judgments j JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
    """).fetchone()

    prod_delta = cur_prod - prev_prod
    sessions_delta = cur_sessions - prev_sessions
    mis_delta = cur_mis - (prev_friction.get("avg_per_session") or cur_mis)

    # Skill level changes
    skill_rows = conn.execute(
        "SELECT dimension_id, current_level FROM skill_profile"
    ).fetchall()
    cur_skills = {r[0]: r[1] for r in skill_rows}
    prev_skills = {}
    prev_synth = conn.execute("""
        SELECT skill_levels FROM synthesis_history ORDER BY id DESC LIMIT 1
    """).fetchone()
    if prev_synth and prev_synth[0]:
        try:
            prev_skills = json.loads(prev_synth[0])
        except Exception:
            pass
    skill_changes = {}
    for dim, level in cur_skills.items():
        prev_level = prev_skills.get(dim, level)
        if level != prev_level:
            skill_changes[dim] = {"from": prev_level, "to": level}

    return jsonify(
        {
            "delta": {
                "previous_run": prev_generated_at,
                "sessions_delta": sessions_delta,
                "productivity_delta": round(prod_delta, 3),
                "misalignment_delta": round(mis_delta, 2),
                "skill_changes": skill_changes,
                "cur_productivity": round(cur_prod, 3),
                "prev_productivity": round(prev_prod, 3),
            }
        }
    )


@app.route("/api/sessions-by-friction")
def api_sessions_by_friction():
    """Return sessions matching a given friction type, for drill-through."""
    friction_type = request.args.get("type", "")
    if not friction_type:
        return jsonify({"sessions": []})

    conn = get_conn()

    # Find keyword patterns for this friction type
    patterns = []
    for name, keywords in _FRICTION_PATTERNS:
        if name == friction_type:
            patterns = keywords
            break

    if not patterns and friction_type != "Other":
        # Fall back to keyword matching using significant words from the title
        stop_words = {
            "the",
            "a",
            "an",
            "and",
            "or",
            "but",
            "in",
            "on",
            "at",
            "to",
            "for",
            "of",
            "with",
            "by",
            "from",
            "is",
            "it",
            "this",
            "that",
            "when",
            "without",
            "before",
            "after",
            "not",
            "what",
            "how",
            "consuming",
            "first",
            "wrong",
            "asking",
            "building",
            "investigating",
        }
        words = [
            w.lower().strip("(),")
            for w in friction_type.split()
            if len(w) > 3 and w.lower() not in stop_words
        ]
        patterns = words[:8]  # Use top 8 keywords
        if not patterns:
            return jsonify({"sessions": [], "error": "Unknown friction type"})

    # Fetch all sessions with misalignments, then filter in Python
    rows = conn.execute("""
        SELECT j.session_id, j.misalignments, j.productivity_ratio, j.outcome,
               j.narrative, j.prompt_summary, s.project_name, s.started_at,
               s.duration_seconds, s.turn_count
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignments IS NOT NULL AND j.misalignments != '[]'
          AND s.turn_count >= 1
        ORDER BY j.misalignment_count DESC
        LIMIT 200
    """).fetchall()

    matched = []
    for row in rows:
        sid, mis_json, prod, outcome, narrative, summary, proj, started, dur, turns = (
            row
        )
        try:
            items = (
                json.loads(mis_json) if isinstance(mis_json, str) else (mis_json or [])
            )
        except Exception:
            items = []

        matching_items = []
        for raw_desc in items:
            # misalignments can be dicts {turn, description} or plain strings
            desc = (
                raw_desc.get("description", "")
                if isinstance(raw_desc, dict)
                else str(raw_desc)
            )
            desc_lower = desc.lower()
            if friction_type == "Other":
                # Check if it matches NO named pattern
                is_other = True
                for _, kws in _FRICTION_PATTERNS:
                    if any(k in desc_lower for k in kws):
                        is_other = False
                        break
                if is_other:
                    matching_items.append(desc)
            else:
                if any(k in desc_lower for k in patterns):
                    matching_items.append(desc)

        if matching_items:
            matched.append(
                {
                    "session_id": sid,
                    "project": proj,
                    "productivity": round(prod or 0, 2),
                    "outcome": outcome,
                    "narrative": narrative,
                    "prompt_summary": summary,
                    "started_at": started,
                    "duration_min": round((dur or 0) / 60),
                    "turns": turns,
                    "matching_frictions": matching_items[:3],
                }
            )

    matched.sort(key=lambda x: x["productivity"])  # worst first
    return jsonify(
        {"sessions": matched[:20], "type": friction_type, "total": len(matched)}
    )


@app.route("/api/sessions/<session_id>/narrative")
def api_session_narrative(session_id):
    """Return the rich narrative for a session."""
    conn = get_conn()
    row = conn.execute(
        """SELECT narrative, what_worked, what_failed, user_quote,
                  claude_md_suggestion, claude_md_rationale, prompt_summary
           FROM session_judgments WHERE session_id = ?""",
        [session_id],
    ).fetchone()

    if not row:
        return jsonify({"narrative": None})

    return jsonify(
        {
            "narrative": {
                "narrative": row[0],
                "what_worked": row[1],
                "what_failed": row[2],
                "user_quote": row[3],
                "claude_md_suggestion": row[4],
                "claude_md_rationale": row[5],
                "prompt_summary": row[6],
            }
        }
    )


@app.route("/api/claude-md-suggestions")
def api_claude_md_suggestions():
    """Return all CLAUDE.md suggestions with copy-ready text."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT j.session_id, j.claude_md_suggestion, j.claude_md_rationale,
               j.prompt_summary, s.project_name, s.started_at
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.claude_md_suggestion IS NOT NULL AND j.claude_md_suggestion != ''
        ORDER BY s.started_at DESC
    """).fetchall()

    # Also include synthesis-level suggestions
    synthesis_suggestions = []
    synth = conn.execute(
        "SELECT claude_md_additions FROM synthesis WHERE id = 1"
    ).fetchone()
    if synth and synth[0]:
        try:
            additions = json.loads(synth[0]) if isinstance(synth[0], str) else synth[0]
            for a in additions:
                synthesis_suggestions.append(
                    {
                        "rule": a.get("rule", ""),
                        "rationale": a.get("rationale", ""),
                        "evidence": a.get("evidence", ""),
                        "source": "synthesis",
                    }
                )
        except (json.JSONDecodeError, ValueError):
            pass

    session_suggestions = []
    for r in rows:
        session_suggestions.append(
            {
                "session_id": r[0],
                "rule": r[1],
                "rationale": r[2],
                "prompt_summary": r[3],
                "project_name": r[4],
                "started_at": _serialize(r[5]),
                "source": "session",
            }
        )

    return jsonify(
        {
            "synthesis_suggestions": synthesis_suggestions,
            "session_suggestions": session_suggestions,
        }
    )


@app.route("/api/session-highlights")
def api_session_highlights():
    """Return top noteworthy sessions with their narratives."""
    conn = get_conn()

    _filter = """
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    highlights = []

    # Most productive session
    row = conn.execute(f"""
        SELECT j.session_id, j.productivity_ratio, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.outcome = 'completed'
        ORDER BY j.productivity_ratio DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append(
            {
                "type": "most_productive",
                "label": "Most Productive",
                "session_id": row[0],
                "productivity": row[1],
                "outcome": row[2],
                "narrative": row[3],
                "prompt_summary": row[4],
                "project": row[5],
                "started_at": _serialize(row[6]),
                "duration": row[7],
                "what_worked": row[8],
            }
        )

    # Most wasteful session
    row = conn.execute(f"""
        SELECT j.session_id, j.productivity_ratio, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_failed, j.misalignment_count
        FROM session_judgments j {_filter}
          AND j.waste_turns > 0
        ORDER BY j.waste_turns DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append(
            {
                "type": "most_wasteful",
                "label": "Most Wasteful",
                "session_id": row[0],
                "productivity": row[1],
                "outcome": row[2],
                "narrative": row[3],
                "prompt_summary": row[4],
                "project": row[5],
                "started_at": _serialize(row[6]),
                "duration": row[7],
                "what_failed": row[8],
                "misalignments": row[9],
            }
        )

    # Most misaligned session
    row = conn.execute(f"""
        SELECT j.session_id, j.misalignment_count, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_failed
        FROM session_judgments j {_filter}
          AND j.misalignment_count > 0
        ORDER BY j.misalignment_count DESC LIMIT 1
    """).fetchone()
    if row and (not highlights or row[0] != highlights[-1].get("session_id")):
        highlights.append(
            {
                "type": "most_misaligned",
                "label": "Most Misaligned",
                "session_id": row[0],
                "misalignments": row[1],
                "outcome": row[2],
                "narrative": row[3],
                "prompt_summary": row[4],
                "project": row[5],
                "started_at": _serialize(row[6]),
                "duration": row[7],
                "what_failed": row[8],
            }
        )

    # Best prompt quality
    row = conn.execute(f"""
        SELECT j.session_id, j.prompt_clarity, j.prompt_completeness, j.outcome,
               j.narrative, j.prompt_summary, s.project_name, s.started_at,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.prompt_clarity >= 0.8 AND j.prompt_completeness >= 0.8
          AND j.outcome = 'completed'
        ORDER BY (j.prompt_clarity + j.prompt_completeness) DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append(
            {
                "type": "best_prompt",
                "label": "Best Prompt",
                "session_id": row[0],
                "clarity": row[1],
                "completeness": row[2],
                "outcome": row[3],
                "narrative": row[4],
                "prompt_summary": row[5],
                "project": row[6],
                "started_at": _serialize(row[7]),
                "what_worked": row[8],
            }
        )

    # Longest successful session
    row = conn.execute(f"""
        SELECT j.session_id, s.duration_seconds, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.turn_count,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.outcome = 'completed'
        ORDER BY s.duration_seconds DESC LIMIT 1
    """).fetchone()
    if row and (not highlights or row[0] != highlights[0].get("session_id")):
        highlights.append(
            {
                "type": "longest_success",
                "label": "Longest Success",
                "session_id": row[0],
                "duration": row[1],
                "outcome": row[2],
                "narrative": row[3],
                "prompt_summary": row[4],
                "project": row[5],
                "started_at": _serialize(row[6]),
                "turns": row[7],
                "what_worked": row[8],
            }
        )

    return jsonify({"highlights": highlights[:5]})


@app.route("/api/time-of-day")
def api_time_of_day():
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            CASE
                WHEN CAST(strftime('%H', timestamp_utc) AS INTEGER) BETWEEN 6 AND 11 THEN 'Morning 6-12'
                WHEN CAST(strftime('%H', timestamp_utc) AS INTEGER) BETWEEN 12 AND 17 THEN 'Afternoon 12-18'
                WHEN CAST(strftime('%H', timestamp_utc) AS INTEGER) BETWEEN 18 AND 23 THEN 'Evening 18-24'
                ELSE 'Night 0-6'
            END as period,
            COUNT(*) as count
        FROM raw_entries
        WHERE entry_type = 'user'
          AND user_text_length > 0
          AND timestamp_utc IS NOT NULL
        GROUP BY period
        ORDER BY count DESC
    """).fetchall()
    return jsonify({"time_of_day": [{"period": r[0], "count": r[1]} for r in rows]})


@app.route("/api/response-times")
def api_response_times():
    """User response latency: time from assistant message to next user message."""
    conn = get_conn()

    # Fetch all inter-message deltas in the plausible human response range
    delta_rows = conn.execute("""
        WITH ordered AS (
            SELECT session_id, entry_type, timestamp_utc,
                LAG(timestamp_utc) OVER (PARTITION BY session_id ORDER BY timestamp_utc) as prev_ts,
                LAG(entry_type) OVER (PARTITION BY session_id ORDER BY timestamp_utc) as prev_type
            FROM raw_entries
            WHERE timestamp_utc IS NOT NULL AND entry_type IN ('user', 'assistant')
        )
        SELECT CAST((julianday(timestamp_utc) - julianday(prev_ts)) * 86400 AS INTEGER) as delta_s
        FROM ordered
        WHERE entry_type = 'user' AND prev_type = 'assistant'
          AND julianday(timestamp_utc) > julianday(prev_ts)
          AND CAST((julianday(timestamp_utc) - julianday(prev_ts)) * 86400 AS INTEGER) BETWEEN 1 AND 86400
        ORDER BY delta_s
    """).fetchall()

    deltas = [r[0] for r in delta_rows]
    if not deltas:
        return jsonify({"distribution": [], "avg_seconds": 0, "median_seconds": 0})

    buckets = [
        ("2-10s", 2, 10),
        ("10-30s", 10, 30),
        ("30s-1m", 30, 60),
        ("1-2m", 60, 120),
        ("2-5m", 120, 300),
        ("5-15m", 300, 900),
        (">15m", 900, 999999),
    ]
    dist = [
        {"label": b, "count": sum(1 for d in deltas if lo <= d < hi)}
        for b, lo, hi in buckets
    ]
    avg_s = sum(deltas) / len(deltas)
    median_s = deltas[len(deltas) // 2]

    return jsonify(
        {
            "distribution": dist,
            "avg_seconds": round(avg_s, 1),
            "median_seconds": round(median_s, 1),
        }
    )


@app.route("/api/multi-clauding")
def api_multi_clauding():
    """Detect overlapping concurrent Claude Code sessions (multi-clauding).

    Two sessions overlap if they both have user messages within the same 5-minute window.
    This is more accurate than comparing session start/end times which span hours.
    """
    conn = get_conn()

    # Find 5-minute windows where 2+ sessions had user activity simultaneously
    # Group user messages into 5-minute buckets and count distinct sessions per bucket
    overlap_rows = conn.execute("""
        WITH bucketed AS (
            SELECT
                session_id,
                CAST(strftime('%s', timestamp_utc) AS INTEGER) / 300 as bucket
            FROM raw_entries
            WHERE entry_type = 'user'
              AND user_text_length > 0
              AND session_id IS NOT NULL
              AND timestamp_utc IS NOT NULL
            GROUP BY session_id, bucket
        ),
        busy_buckets AS (
            SELECT bucket, COUNT(DISTINCT session_id) as concurrent_sessions,
                   GROUP_CONCAT(DISTINCT session_id) as session_list
            FROM bucketed
            GROUP BY bucket
            HAVING COUNT(DISTINCT session_id) >= 2
        )
        SELECT COUNT(*) as overlap_events,
               COUNT(DISTINCT session_id) as sessions_involved
        FROM busy_buckets
        JOIN bucketed ON bucketed.bucket = busy_buckets.bucket
    """).fetchone()

    overlap_events = overlap_rows[0] or 0
    sessions_involved = overlap_rows[1] or 0

    return jsonify(
        {
            "overlap_events": overlap_events,
            "sessions_involved": sessions_involved,
            "sessions_involved_pct": round(
                sessions_involved
                * 100
                / max(
                    conn.execute(
                        "SELECT COUNT(DISTINCT session_id) FROM sessions"
                    ).fetchone()[0],
                    1,
                )
            ),
        }
    )


@app.route("/api/friction")
def api_friction():
    """Friction type distribution from session judgments."""
    conn = get_conn()

    # Parse misalignments JSON arrays to extract friction categories
    rows = conn.execute("""
        SELECT misalignments
        FROM session_judgments
        WHERE misalignments IS NOT NULL AND misalignments != '' AND misalignments != '[]'
    """).fetchall()

    import json as _json
    from collections import Counter

    # Priority-ordered patterns; first match wins, minimising "Other"
    # Priority-ordered patterns defined at module level

    counter: Counter = Counter()
    for (raw,) in rows:
        try:
            descs = _json.loads(raw)
            if not isinstance(descs, list):
                continue
            for item in descs:
                desc_l = (
                    item.get("description", "") if isinstance(item, dict) else str(item)
                ).lower()
                label = _categorize_friction(desc_l)
                counter[label] += 1
        except Exception:
            continue

    # Keep "Other" as a true residual bucket.
    if "Other" in counter:
        non_other_counts = [v for k, v in counter.items() if k != "Other" and v > 0]
        if non_other_counts:
            counter["Other"] = min(counter["Other"], max(0, min(non_other_counts) - 1))
            if counter["Other"] == 0:
                del counter["Other"]

    # Sort by count descending, but always put "Other" last
    sorted_items = sorted(
        ((k, v) for k, v in counter.items() if k != "Other"), key=lambda x: -x[1]
    )
    if "Other" in counter:
        sorted_items.append(("Other", counter["Other"]))
    items = [{"label": k, "count": v} for k, v in sorted_items]
    return jsonify({"friction": items})


@app.route("/api/tool-errors")
def api_tool_errors():
    """Tool error type distribution."""
    conn = get_conn()

    # Check if tool_result_error_type column exists
    cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_entries)").fetchall()}
    if "tool_result_error_type" not in cols:
        return jsonify({"tool_errors": [], "needs_reingest": True})

    rows = conn.execute("""
        SELECT tool_result_error_type, COUNT(*) as count
        FROM raw_entries
        WHERE tool_result_error = 1 AND tool_result_error_type IS NOT NULL
        GROUP BY tool_result_error_type
        ORDER BY count DESC
    """).fetchall()
    return jsonify(
        {
            "tool_errors": [
                {"label": r[0].replace("_", " ").title(), "count": r[1]} for r in rows
            ],
            "needs_reingest": len(rows) == 0,
        }
    )


@app.route("/api/languages")
def api_languages():
    """Language breakdown from file edits."""
    conn = get_conn()

    # Check if session_languages table exists
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "session_languages" not in tables:
        return jsonify({"languages": [], "needs_reingest": True})

    # Only recognised code/config/markup extensions — no images, binaries, or tool outputs
    _CODE_EXTS = {
        "py",
        "ts",
        "tsx",
        "js",
        "jsx",
        "mjs",
        "cjs",
        "html",
        "htm",
        "css",
        "scss",
        "sass",
        "md",
        "mdx",
        "rst",
        "json",
        "yaml",
        "yml",
        "toml",
        "ini",
        "cfg",
        "env",
        "sh",
        "bash",
        "zsh",
        "fish",
        "go",
        "rs",
        "rb",
        "java",
        "kt",
        "swift",
        "c",
        "cpp",
        "h",
        "hpp",
        "sql",
        "prisma",
        "graphql",
        "gql",
        "ipynb",
        "r",
        "scala",
        "clj",
        "ex",
        "exs",
        "erl",
        "hs",
        "lua",
        "tf",
        "hcl",
        "dockerfile",
    }
    _LABEL_MAP = {
        "py": "Python",
        "ts": "TypeScript",
        "tsx": "TSX",
        "js": "JavaScript",
        "jsx": "JSX",
        "mjs": "JavaScript",
        "cjs": "JavaScript",
        "md": "Markdown",
        "mdx": "MDX",
        "rst": "reStructuredText",
        "html": "HTML",
        "htm": "HTML",
        "css": "CSS",
        "scss": "SCSS",
        "sass": "Sass",
        "json": "JSON",
        "yaml": "YAML",
        "yml": "YAML",
        "sh": "Shell",
        "bash": "Shell",
        "zsh": "Shell",
        "go": "Go",
        "rs": "Rust",
        "java": "Java",
        "rb": "Ruby",
        "kt": "Kotlin",
        "swift": "Swift",
        "c": "C",
        "cpp": "C++",
        "h": "C/C++",
        "sql": "SQL",
        "toml": "TOML",
        "ipynb": "Notebook",
        "tf": "Terraform",
        "hcl": "HCL",
        "dockerfile": "Dockerfile",
        "graphql": "GraphQL",
        "gql": "GraphQL",
        "prisma": "Prisma",
        "scala": "Scala",
        "r": "R",
    }

    rows = conn.execute("""
        SELECT extension, SUM(file_count) as total
        FROM session_languages
        WHERE extension != ''
        GROUP BY extension
        ORDER BY total DESC
        LIMIT 50
    """).fetchall()

    # Merge rows that share the same display label (e.g. yaml + yml → YAML)
    merged: dict[str, int] = {}
    for r in rows:
        if r[0] not in _CODE_EXTS:
            continue
        label = _LABEL_MAP.get(r[0], r[0].upper())
        merged[label] = merged.get(label, 0) + r[1]
    items = [
        {"label": k, "count": v} for k, v in sorted(merged.items(), key=lambda x: -x[1])
    ][:12]
    return jsonify({"languages": items, "needs_reingest": len(items) == 0})


@app.route("/api/bash-commands")
def api_bash_commands():
    """Top bash commands by extracting the first word/program from bash tool_input_preview."""
    import re as _re

    conn = get_conn()

    # Check if tool_input_preview column exists
    cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_entries)").fetchall()}
    if "tool_input_preview" not in cols:
        return jsonify({"bash_commands": [], "needs_reingest": True})

    rows = conn.execute("""
        SELECT tool_input_preview
        FROM raw_entries
        WHERE tool_names LIKE '%Bash%'
          AND tool_input_preview IS NOT NULL
          AND tool_input_preview != ''
          AND is_tool_result = 0
    """).fetchall()

    # Extract the first actual command from the preview
    _STRIP_PREFIXES = ("nohup ", "sudo ", "eval ", "env ", "time ")
    from collections import Counter

    cmd_counter: Counter = Counter()

    for (preview,) in rows:
        if not preview:
            continue
        # Strip leading whitespace, shell var assignments like FOO=bar cmd
        cmd = preview.strip()
        # Strip env var assignments: VAR=value cmd -> cmd
        cmd = _re.sub(r"^([A-Z_][A-Z0-9_]*=[^\s]*\s+)+", "", cmd)
        # Strip common wrappers
        for prefix in _STRIP_PREFIXES:
            if cmd.startswith(prefix):
                cmd = cmd[len(prefix) :]
        # Get first token
        first = _re.split(r"[\s|;&]", cmd)[0].strip()
        # Strip shell path prefix (e.g., /usr/bin/grep -> grep)
        if "/" in first:
            first = first.rsplit("/", 1)[-1]
        # Normalise
        first = first.lower().strip("\"' ")
        if not first or len(first) > 40:
            continue
        # Skip numbers and special chars only
        if not _re.search(r"[a-z]", first):
            continue
        cmd_counter[first] += 1

    items = [
        {"label": cmd, "value": count} for cmd, count in cmd_counter.most_common(20)
    ]
    return jsonify({"bash_commands": items})


@app.route("/api/heatmap")
def api_heatmap():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            f.day_of_week,
            f.hour_of_day,
            COUNT(*) as count,
            AVG(s.convergence_score) as avg_convergence
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY f.day_of_week, f.hour_of_day
        ORDER BY f.day_of_week, f.hour_of_day
    """).fetchall()

    return jsonify(
        {
            "heatmap": [
                {
                    "day": r[0],
                    "hour": r[1],
                    "count": r[2],
                    "avg_convergence": round(r[3], 3),
                }
                for r in rows
            ],
        }
    )


@app.route("/api/heatmap/calendar")
def api_heatmap_calendar():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            DATE(started_at) as day,
            COUNT(*) as count
        FROM sessions
        WHERE turn_count >= 1
          AND first_prompt NOT LIKE 'You are analyzing a Claude Code session%%'
          AND started_at >= DATE('now', '-365 days')
        GROUP BY DATE(started_at)
        ORDER BY day
    """).fetchall()

    return jsonify(
        {
            "calendar": [{"date": str(r[0]), "count": r[1]} for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# New features: Groundhog Day, Lost Hours, Streaks, etc.
# ---------------------------------------------------------------------------


def _categorize_friction(desc_lower: str) -> str:
    """Categorize a misalignment description into a friction pattern name."""
    text = (desc_lower or "").lower().strip()
    if not text:
        return "Other"

    for name, keywords in _FRICTION_PATTERNS:
        if any(kw in text for kw in keywords):
            return name

    # Heuristic fallback: avoid dumping most cases into "Other".
    if any(
        kw in text
        for kw in (
            "wrong",
            "instead of",
            "incorrect",
            "bad approach",
            "off track",
            "off-track",
            "diverged",
            "unnecessary",
            "scope creep",
        )
    ):
        return "Wrong Approach"
    if any(
        kw in text
        for kw in (
            "retry",
            "repeated",
            "again",
            "still failing",
            "consecutive",
            "loop",
            "stuck",
            "circular",
        )
    ):
        return "Repeated Failure"
    if any(
        kw in text
        for kw in (
            "user said",
            "user requested",
            "despite",
            "ignored",
            "instruction",
            "as asked",
            "as requested",
        )
    ):
        return "Ignored User Instruction"

    # Last resort for non-empty text.
    return "Misunderstood Request"


@app.route("/api/groundhog-day")
def api_groundhog_day():
    """Detect repeated friction patterns across sessions (Groundhog Day detector)."""
    conn = get_conn()

    rows = conn.execute("""
        SELECT j.session_id, j.misalignments, j.user_quote, j.prompt_summary,
               s.project_name, s.started_at
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignments IS NOT NULL AND j.misalignments != '[]'
          AND s.turn_count >= 3
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
        ORDER BY s.started_at
    """).fetchall()

    # Group by (project_name, friction_category)
    from collections import defaultdict

    groups = defaultdict(list)
    for sid, mis_json, user_quote, prompt_summary, project, started_at in rows:
        try:
            items = (
                json.loads(mis_json) if isinstance(mis_json, str) else (mis_json or [])
            )
        except Exception:
            continue
        short_project = (
            (project or "").replace("-Users-npow-code-", "").replace("-Users-npow-", "")
        )
        seen_cats = set()
        for item in items:
            desc = item.get("description", "") if isinstance(item, dict) else str(item)
            cat = _categorize_friction(desc.lower())
            if cat not in seen_cats:
                seen_cats.add(cat)
                key = (short_project, cat)
                groups[key].append(
                    {
                        "session_id": sid,
                        "date": str(started_at)[:10] if started_at else "",
                        "user_quote": (user_quote or "")[:120],
                        "prompt_summary": (prompt_summary or "")[:100],
                    }
                )

    loops = []
    for (project, pattern), sessions in groups.items():
        if len(sessions) >= 2:
            loops.append(
                {
                    "project": project,
                    "pattern": pattern,
                    "count": len(sessions),
                    "sessions": sessions[-5:],  # most recent 5
                }
            )

    loops.sort(key=lambda x: -x["count"])
    return jsonify({"loops": loops})


@app.route("/api/lost-hours")
def api_lost_hours():
    """Calculate cumulative hours lost to friction by category."""
    conn = get_conn()

    rows = conn.execute("""
        SELECT j.waste_turns, j.misalignments, s.duration_seconds, s.turn_count,
               j.estimated_cost_usd
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.waste_turns > 0 AND s.turn_count > 0
          AND s.duration_seconds > 0
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """).fetchall()

    from collections import defaultdict

    cat_data = defaultdict(lambda: {"waste_hours": 0.0, "sessions": 0, "cost_usd": 0.0})
    total_waste_hours = 0.0
    total_cost_usd = 0.0

    for waste_turns, mis_json, dur_s, turn_count, cost_usd in rows:
        turn_duration_s = dur_s / turn_count if turn_count > 0 else 0
        session_waste_hours = (waste_turns * turn_duration_s) / 3600.0

        # Distribute waste proportionally across misalignment categories in session
        cats_in_session = set()
        if mis_json:
            try:
                items = (
                    json.loads(mis_json)
                    if isinstance(mis_json, str)
                    else (mis_json or [])
                )
                for item in items:
                    desc = (
                        item.get("description", "")
                        if isinstance(item, dict)
                        else str(item)
                    )
                    cats_in_session.add(_categorize_friction(desc.lower()))
            except Exception:
                pass

        if not cats_in_session:
            cats_in_session = {"Other"}

        per_cat_hours = session_waste_hours / len(cats_in_session)
        per_cat_cost = (cost_usd or 0) / len(cats_in_session)
        for c in cats_in_session:
            cat_data[c]["waste_hours"] += per_cat_hours
            cat_data[c]["sessions"] += 1
            cat_data[c]["cost_usd"] += per_cat_cost

        total_waste_hours += session_waste_hours
        total_cost_usd += cost_usd or 0

    by_category = sorted(
        [
            {
                "category": cat,
                "waste_hours": round(data["waste_hours"], 2),
                "sessions": data["sessions"],
                "cost_usd": round(data["cost_usd"], 2),
            }
            for cat, data in cat_data.items()
        ],
        key=lambda x: -x["waste_hours"],
    )

    return jsonify(
        {
            "total_waste_hours": round(total_waste_hours, 2),
            "total_cost_usd": round(total_cost_usd, 2),
            "by_category": by_category,
        }
    )


@app.route("/api/streaks")
def api_streaks():
    """Calculate clean session streaks by friction category."""
    conn = get_conn()

    rows = conn.execute("""
        SELECT j.session_id, j.misalignments, s.started_at
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
        ORDER BY s.started_at
    """).fetchall()

    if not rows:
        return jsonify({"streaks": []})

    # Build timeline: for each session, which friction categories appeared
    session_cats = []
    for sid, mis_json, started_at in rows:
        cats = set()
        if mis_json:
            try:
                items = (
                    json.loads(mis_json)
                    if isinstance(mis_json, str)
                    else (mis_json or [])
                )
                for item in items:
                    desc = (
                        item.get("description", "")
                        if isinstance(item, dict)
                        else str(item)
                    )
                    cats.add(_categorize_friction(desc.lower()))
            except Exception:
                pass
        session_cats.append((str(started_at)[:10] if started_at else "", cats))

    # For each friction category: compute current streak, best streak, last occurrence
    all_cats = set()
    for _, cats in session_cats:
        all_cats.update(cats)

    from datetime import date as _date

    streaks = []
    for cat in sorted(all_cats):
        if cat == "Other":
            continue

        # Current streak: consecutive sessions from the end WITHOUT this category
        current_streak = 0
        for day_str, cats in reversed(session_cats):
            if cat in cats:
                break
            current_streak += 1

        # Best streak: longest consecutive run without this category
        best_streak = 0
        run = 0
        last_occurrence_date = ""
        for day_str, cats in session_cats:
            if cat in cats:
                best_streak = max(best_streak, run)
                run = 0
                last_occurrence_date = day_str
            else:
                run += 1
        best_streak = max(best_streak, run)

        # Days since last occurrence
        days_since = None
        if last_occurrence_date:
            try:
                from datetime import datetime

                last_dt = datetime.strptime(last_occurrence_date, "%Y-%m-%d").date()
                today_dt = _date.today()
                days_since = (today_dt - last_dt).days
            except Exception:
                pass

        streaks.append(
            {
                "pattern": cat,
                "current_streak": current_streak,
                "best_streak": best_streak,
                "last_occurrence": last_occurrence_date,
                "days_since": days_since,
            }
        )

    streaks.sort(key=lambda x: -x["current_streak"])
    return jsonify({"streaks": streaks})


@app.route("/api/friction-pattern-map")
def api_friction_pattern_map():
    """Cross-project friction matrix."""
    conn = get_conn()

    rows = conn.execute("""
        SELECT j.misalignments, s.project_name
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignments IS NOT NULL AND j.misalignments != '[]'
          AND s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """).fetchall()

    from collections import defaultdict

    matrix = defaultdict(lambda: defaultdict(int))
    projects_set = set()
    patterns_set = set()

    for mis_json, project in rows:
        short_project = (
            (project or "").replace("-Users-npow-code-", "").replace("-Users-npow-", "")
        )
        try:
            items = (
                json.loads(mis_json) if isinstance(mis_json, str) else (mis_json or [])
            )
            for item in items:
                desc = (
                    item.get("description", "") if isinstance(item, dict) else str(item)
                )
                cat = _categorize_friction(desc.lower())
                matrix[short_project][cat] += 1
                projects_set.add(short_project)
                patterns_set.add(cat)
        except Exception:
            pass

    # Sort projects by total friction count
    projects = sorted(projects_set, key=lambda p: -sum(matrix[p].values()))[:10]
    patterns = sorted(
        patterns_set, key=lambda pa: -sum(matrix[p].get(pa, 0) for p in projects)
    )

    matrix_out = {p: dict(matrix[p]) for p in projects}
    return jsonify(
        {
            "projects": projects,
            "patterns": patterns,
            "matrix": matrix_out,
        }
    )


@app.route("/api/claudemd-effectiveness")
def api_claudemd_effectiveness():
    """Track effectiveness of CLAUDE.md rules over time."""
    conn = get_conn()

    # Get current rules from synthesis
    synth_row = conn.execute(
        "SELECT claude_md_additions FROM synthesis WHERE id = 1"
    ).fetchone()
    if not synth_row or not synth_row[0]:
        return jsonify({"effectiveness": []})

    try:
        additions = (
            json.loads(synth_row[0])
            if isinstance(synth_row[0], str)
            else (synth_row[0] or [])
        )
    except (json.JSONDecodeError, ValueError):
        return jsonify({"effectiveness": []})

    # Get synthesis history ordered by time
    history_rows = conn.execute("""
        SELECT claude_md_additions, generated_at FROM synthesis_history
        ORDER BY generated_at ASC
    """).fetchall()

    # Get all sessions ordered by time with misalignment data
    session_rows = conn.execute("""
        SELECT s.started_at, j.misalignment_count, j.misalignments
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
        ORDER BY s.started_at
    """).fetchall()

    results = []
    for addition in additions:
        rule = addition.get("rule", "").strip()
        if not rule:
            continue

        # Find first time this rule appeared in history
        first_seen = None
        for hist_json, gen_at in history_rows:
            if hist_json and rule[:40].lower() in (hist_json or "").lower():
                first_seen = gen_at
                break

        # Count sessions before/after with any friction
        before_total = before_friction = after_total = after_friction = 0
        for started_at, mis_count, _ in session_rows:
            if first_seen and str(started_at) < str(first_seen):
                before_total += 1
                if mis_count and mis_count > 0:
                    before_friction += 1
            else:
                after_total += 1
                if mis_count and mis_count > 0:
                    after_friction += 1

        before_rate = before_friction / before_total if before_total > 0 else None
        after_rate = after_friction / after_total if after_total > 0 else None

        if before_rate is not None and after_rate is not None:
            delta = after_rate - before_rate
            if delta < -0.1:
                status = "working"
            elif delta > 0.05:
                status = "violated"
            else:
                status = "unclear"
        elif first_seen is None:
            status = "new"
        else:
            status = "insufficient_data"

        results.append(
            {
                "rule": rule,
                "first_seen": first_seen,
                "before_sessions": before_total,
                "after_sessions": after_total,
                "before_friction_rate": round(before_rate, 2)
                if before_rate is not None
                else None,
                "after_friction_rate": round(after_rate, 2)
                if after_rate is not None
                else None,
                "delta": round(delta, 2)
                if (before_rate is not None and after_rate is not None)
                else None,
                "status": status,
            }
        )

    return jsonify({"effectiveness": results})


@app.route("/api/sessions/<session_id>/rewrite-prompt", methods=["POST"])
def api_rewrite_prompt(session_id):
    """Generate a rewritten version of the session's opening prompt."""
    from .llm_judge import rewrite_prompt

    conn = get_conn()
    result = rewrite_prompt(session_id, conn)
    return jsonify(result)


@app.route("/api/sessions/<session_id>/handoff")
def api_session_handoff(session_id):
    """Generate a handoff memo for a session."""
    from .llm_judge import generate_handoff

    conn = get_conn()
    result = generate_handoff(session_id, conn)
    return jsonify(result)


@app.route("/api/predict-friction", methods=["POST"])
def api_predict_friction():
    """Predict friction risk for a new prompt."""
    from .llm_judge import predict_friction

    body = request.get_json(silent=True) or {}
    prompt_text = body.get("prompt", "").strip()
    if not prompt_text:
        return jsonify({"error": "prompt is required"}), 400
    conn = get_conn()
    result = predict_friction(prompt_text, conn)
    return jsonify(result)


@app.route("/api/claudemd-audit", methods=["POST"])
def api_claudemd_audit():
    """Audit CLAUDE.md rules against recent session data."""
    from .llm_judge import audit_claudemd

    conn = get_conn()
    result = audit_claudemd(conn)
    return jsonify(result)


# ---------------------------------------------------------------------------
# Live agent monitor (process-monitor view)
# ---------------------------------------------------------------------------

_PROCESS_MONITOR_JS = (
    Path.home()
    / "code"
    / "sessionlog"
    / "views"
    / "process-monitor"
    / "dist"
    / "index.js"
)


@app.route("/viewer")
def viewer():
    return send_from_directory(_static, "viewer.html")


@app.route("/viewer/process-monitor.js")
def viewer_process_monitor_js():
    content = _PROCESS_MONITOR_JS.read_text()
    return Response(content, mimetype="application/javascript")


@app.route("/api/session/<session_id>/dag")
def api_session_dag(session_id):
    """Return the execution DAG for a session.

    Each turn = one assistant message (is_sidechain=0).
    For turns containing a Task call, include the sub-agent's tool sequence
    from progress_entries (parent_tool_id = the assistant entry_id).
    """
    import json as _json

    conn = get_conn()

    # Ensure indexes exist (created once, fast no-op thereafter)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_progress_parent
        ON progress_entries(parent_tool_id, progress_type)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_parent_uuid
        ON raw_entries(parent_uuid, is_tool_result)
    """)

    # Main-thread turns (not sidechain), most recent 150 turns
    turns_rows = conn.execute(
        """
        SELECT r.entry_id, r.timestamp_utc, r.tool_names, r.tool_input_preview,
               r.duration_ms,
               CASE WHEN tr.entry_id IS NOT NULL THEN 1 ELSE 0 END AS completed,
               COALESCE(tr.tool_result_error, 0) AS error
        FROM (
            SELECT entry_id, timestamp_utc, tool_names, tool_input_preview, duration_ms
            FROM raw_entries
            WHERE session_id = ?
              AND entry_type = 'assistant'
              AND is_sidechain = 0
              AND tool_names IS NOT NULL
              AND tool_names != '[]'
            ORDER BY timestamp_utc DESC
            LIMIT 150
        ) r
        LEFT JOIN raw_entries tr
               ON tr.parent_uuid = r.entry_id AND tr.is_tool_result = 1
        ORDER BY r.timestamp_utc
    """,
        [session_id],
    ).fetchall()

    # Fetch ALL agent_progress entries for this session in one query, then
    # trace chains in Python (avoids N slow recursive CTEs).
    # Chain structure: first entry has parent_tool_id = raw Task entry_id;
    # subsequent entries chain: parent_tool_id = previous progress entry_id.
    all_prog = conn.execute(
        """
        SELECT entry_id, parent_tool_id, tool_name, has_result, result_error, timestamp_utc
        FROM progress_entries
        WHERE session_id = ? AND progress_type = 'agent_progress'
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    # Build lookup: parent_tool_id → [children]
    prog_children: dict = {}
    for r in all_prog:
        prog_children.setdefault(r[1], []).append(r)

    def collect_chain(root_id: str) -> list:
        """Walk the chain rooted at root_id, collecting tool_name entries."""
        result = []
        stack = list(prog_children.get(root_id, []))
        visited = set()
        while stack:
            row = stack.pop(0)
            eid = row[0]
            if eid in visited:
                continue
            visited.add(eid)
            if row[2]:  # tool_name is not None
                result.append(
                    {
                        "tool": row[2],
                        "completed": bool(row[3]),
                        "error": bool(row[4]),
                        "timestamp": row[5],
                    }
                )
            stack.extend(prog_children.get(eid, []))
        result.sort(key=lambda x: x["timestamp"])
        return result

    # Build turns
    turns = []
    for (
        entry_id,
        ts,
        tool_names_json,
        preview,
        duration_ms,
        completed,
        error,
    ) in turns_rows:
        try:
            names = _json.loads(tool_names_json) if tool_names_json else []
        except Exception:
            names = []

        subagent_calls = None
        if "Task" in names:
            subagent_calls = collect_chain(entry_id)

        turns.append(
            {
                "entryId": entry_id,
                "timestamp": ts,
                "tools": names,
                "preview": (preview or "").strip()[:120],
                "durationMs": duration_ms,
                "completed": bool(completed),
                "error": bool(error),
                "subagentCalls": subagent_calls,  # None if no Task in this turn
            }
        )

    return jsonify({"sessionId": session_id, "turns": turns})


@app.route("/api/session/<session_id>/subagents")
def api_session_subagents(session_id):
    """Return Task/subagent calls within a session, with completion status."""
    import json as _json

    conn = get_conn()

    rows = conn.execute(
        """
        SELECT
            r.entry_id,
            r.timestamp_utc                                  AS started_at,
            r.tool_input_preview,
            r.tool_names,
            CASE WHEN tr.entry_id IS NOT NULL THEN 1 ELSE 0 END AS completed,
            COALESCE(tr.tool_result_error, 0)                AS error,
            tr.timestamp_utc                                 AS finished_at,
            SUBSTR(COALESCE(tr.user_text, ''), 1, 300)      AS output
        FROM raw_entries r
        LEFT JOIN raw_entries tr
               ON tr.parent_uuid = r.entry_id
              AND tr.is_tool_result = 1
        WHERE r.session_id = ?
          AND r.entry_type = 'assistant'
          AND (
                r.tool_names LIKE '%Task%'
             OR r.tool_names LIKE '%Bash%'
             OR r.tool_names LIKE '%WebSearch%'
             OR r.tool_names LIKE '%WebFetch%'
             OR r.tool_names LIKE '%Read%'
             OR r.tool_names LIKE '%Grep%'
             OR r.tool_names LIKE '%Glob%'
             OR r.tool_names LIKE '%Edit%'
             OR r.tool_names LIKE '%Write%'
          )
        ORDER BY r.timestamp_utc
    """,
        [session_id],
    ).fetchall()

    agents = []
    for (
        entry_id,
        started_at,
        preview,
        tool_names_json,
        completed,
        error,
        finished_at,
        output,
    ) in rows:
        try:
            names = _json.loads(tool_names_json) if tool_names_json else []
        except Exception:
            names = []
        tool = names[0] if names else "?"

        # duration
        duration_ms = None
        if started_at and finished_at:
            try:
                from datetime import datetime as _dt

                s = _dt.fromisoformat(started_at.replace("Z", "+00:00"))
                f = _dt.fromisoformat(finished_at.replace("Z", "+00:00"))
                duration_ms = int((f - s).total_seconds() * 1000)
            except Exception:
                pass

        agents.append(
            {
                "id": entry_id,
                "tool": tool,
                "prompt": (preview or "").strip()[:120],
                "output": (output or "").strip()[:300],
                "completed": bool(completed),
                "error": bool(error),
                "startedAt": started_at,
                "finishedAt": finished_at,
                "durationMs": duration_ms,
            }
        )

    return jsonify({"sessionId": session_id, "agents": agents})


@app.route("/api/live")
def api_live():
    import json as _json

    conn = get_conn()

    _PRICING = {
        "claude-opus-4-6": (15.0, 75.0),
        "claude-sonnet-4-6": (3.0, 15.0),
        "claude-sonnet-4-5": (3.0, 15.0),
        "claude-haiku-4-5-20251001": (0.8, 4.0),
        "claude-haiku-4-5": (0.8, 4.0),
        "claude-3-5-sonnet-20241022": (3.0, 15.0),
        "claude-3-7-sonnet-20250219": (3.0, 15.0),
        "claude-3-5-haiku-20241022": (0.8, 4.0),
        "claude-3-opus-20240229": (15.0, 75.0),
    }

    def cost_usd(model, inp, out):
        ip, op = _PRICING.get(model, (3.0, 15.0))
        return (inp * ip + out * op) / 1_000_000

    # Main query: all sessions active in last 10 min, with rich state
    rows = conn.execute("""
        WITH recent_sessions AS (
            -- Only sessions with activity in the last 3 minutes.
            -- A session is "active" only if something actually happened recently;
            -- finished sessions drop off after 3 min rather than lingering as false
            -- "waiting" entries. The is_running check separately uses a 15-min
            -- cutoff to handle slow Bash/Task calls.
            SELECT DISTINCT session_id
            FROM raw_entries
            WHERE julianday(timestamp_utc) > julianday('now', '-8 hours')
              AND session_id IS NOT NULL
        ),
        session_stats AS (
            SELECT
                session_id,
                COUNT(*) FILTER (WHERE entry_type = 'assistant')           AS turn_count,
                SUM(input_tokens)                                           AS total_in,
                SUM(output_tokens)                                          AS total_out,
                SUM(tool_result_error)                                      AS error_count,
                MAX(model) FILTER (WHERE model LIKE 'claude-%')             AS model,
                -- peak context in last 5 min; cast guards against corrupt rows
                MAX(CAST(input_tokens AS INTEGER)) FILTER (
                    WHERE entry_type = 'assistant'
                      AND julianday(timestamp_utc) > julianday('now', '-300 seconds')
                      AND typeof(input_tokens) IN ('integer', 'real')
                )                                                           AS recent_ctx
            FROM raw_entries
            WHERE session_id IN (SELECT session_id FROM recent_sessions)
            GROUP BY session_id
        ),
        -- Most recent assistant entry that had tool calls (only within 15 min —
        -- older unmatched calls are stale ingestion artifacts, not live activity)
        latest_tool_asst AS (
            SELECT e.session_id, e.entry_id,
                   e.tool_names, e.timestamp_utc AS tool_ts,
                   e.tool_file_paths, e.tool_input_preview
            FROM raw_entries e
            WHERE e.session_id IN (SELECT session_id FROM recent_sessions)
              AND e.entry_type = 'assistant'
              AND e.tool_names IS NOT NULL
              AND e.tool_names != '[]'
              AND e.tool_names != ''
              AND julianday(e.timestamp_utc) > julianday('now', '-900 seconds')
              AND e.timestamp_utc = (
                  SELECT MAX(r2.timestamp_utc) FROM raw_entries r2
                  WHERE r2.session_id = e.session_id
                    AND r2.entry_type = 'assistant'
                    AND r2.tool_names IS NOT NULL
                    AND r2.tool_names != '[]'
                    AND r2.tool_names != ''
                    AND julianday(r2.timestamp_utc) > julianday('now', '-900 seconds')
              )
        ),
        -- Last assistant text snippet (what the agent was explaining/thinking)
        last_text AS (
            SELECT e.session_id,
                   e.text_content AS snippet
            FROM raw_entries e
            WHERE e.session_id IN (SELECT session_id FROM recent_sessions)
              AND e.entry_type = 'assistant'
              AND e.text_length > 0
              AND e.timestamp_utc = (
                  SELECT MAX(r2.timestamp_utc) FROM raw_entries r2
                  WHERE r2.session_id = e.session_id
                    AND r2.entry_type = 'assistant'
                    AND r2.text_length > 0
              )
        )
        SELECT
            rs.session_id,
            lta.tool_names,
            lta.tool_ts                                                     AS tool_started_at,
            -- running = latest tool call (within 15 min) has no result yet
            CASE WHEN lta.entry_id IS NOT NULL
                      AND tr.entry_id IS NULL THEN 1 ELSE 0 END             AS is_running,
            COALESCE(s.project_name,
                (SELECT project_name FROM raw_entries
                 WHERE session_id = rs.session_id
                   AND project_name IS NOT NULL LIMIT 1))                   AS project_name,
            COALESCE(s.started_at,
                (SELECT MIN(timestamp_utc) FROM raw_entries
                 WHERE session_id = rs.session_id))                         AS started_at,
            (SELECT MAX(timestamp_utc) FROM raw_entries
             WHERE session_id = rs.session_id)                              AS last_active,
            COALESCE(s.first_prompt, '')                                    AS first_prompt,
            COALESCE(ss.turn_count,   0)                                    AS turn_count,
            COALESCE(ss.total_in,     0)                                    AS total_in,
            COALESCE(ss.total_out,    0)                                    AS total_out,
            COALESCE(ss.error_count,  0)                                    AS error_count,
            ss.model,
            COALESCE(ss.recent_ctx,   0)                                    AS recent_ctx,
            (SELECT cwd FROM raw_entries
             WHERE session_id = rs.session_id
               AND cwd IS NOT NULL
             ORDER BY timestamp_utc DESC LIMIT 1)                           AS cwd,
            lta.tool_file_paths                                             AS tool_file_paths,
            COALESCE(lta.tool_input_preview, '')                           AS tool_input_preview,
            SUBSTR(COALESCE(lt.snippet, ''), 1, 120)                       AS last_snippet
        FROM recent_sessions rs
        LEFT JOIN latest_tool_asst lta    ON lta.session_id = rs.session_id
        LEFT JOIN raw_entries tr          ON tr.parent_uuid = lta.entry_id
                                         AND tr.is_tool_result = 1
        LEFT JOIN sessions s              ON s.session_id   = rs.session_id
        LEFT JOIN session_stats ss        ON ss.session_id  = rs.session_id
        LEFT JOIN last_text lt            ON lt.session_id  = rs.session_id
        ORDER BY last_active DESC
    """).fetchall()

    # Separate query: current error streak per session
    # (count of consecutive errors from the most recent tool result backward)
    streak_rows = conn.execute("""
        WITH recent_sessions AS (
            SELECT DISTINCT session_id FROM raw_entries
            WHERE julianday(timestamp_utc) > julianday('now', '-8 hours')
              AND session_id IS NOT NULL
        ),
        ranked AS (
            SELECT session_id, tool_result_error,
                   ROW_NUMBER() OVER (
                       PARTITION BY session_id ORDER BY timestamp_utc DESC
                   ) AS rn
            FROM raw_entries
            WHERE session_id IN (SELECT session_id FROM recent_sessions)
              AND is_tool_result = 1
        ),
        first_success AS (
            SELECT session_id, MIN(rn) AS success_rn
            FROM ranked WHERE tool_result_error = 0
            GROUP BY session_id
        )
        SELECT r.session_id, COUNT(*) AS streak
        FROM ranked r
        LEFT JOIN first_success fs ON fs.session_id = r.session_id
        WHERE r.tool_result_error = 1
          AND r.rn < COALESCE(fs.success_rn, 99999)
        GROUP BY r.session_id
    """).fetchall()
    streaks = {r[0]: r[1] for r in streak_rows}

    agents = []
    for r in rows:
        (
            session_id,
            tool_names_json,
            tool_started_at,
            is_running,
            project_name,
            started_at,
            last_active,
            first_prompt,
            turn_count,
            total_in,
            total_out,
            error_count,
            model,
            recent_ctx,
            cwd,
            tool_file_paths_json,
            tool_input_preview,
            last_snippet,
        ) = r

        try:
            names = _json.loads(tool_names_json) if tool_names_json else []
        except Exception:
            names = []

        try:
            file_paths = (
                _json.loads(tool_file_paths_json) if tool_file_paths_json else []
            )
        except Exception:
            file_paths = []

        current_tool = names[0] if names else None

        # Build "what it's doing" context string:
        # 1. Prefer tool_input_preview (actual command/query/prompt from ingest)
        # 2. Fall back to file basename for file ops
        # 3. Fall back to last assistant text snippet
        context = ""
        if tool_input_preview:
            context = tool_input_preview[:120]
        elif current_tool and file_paths:
            import os as _os

            context = _os.path.basename(file_paths[0])
        elif last_snippet:
            context = last_snippet.strip().split("\n")[0].strip()[:80]

        status = "active" if is_running else "idle"

        if first_prompt and first_prompt.strip():
            label = first_prompt.strip().split("\n")[0].strip()[:72]
        else:
            label = project_name or session_id[:8] or "unknown"

        try:
            ctx_pct = (
                round(int(recent_ctx or 0) / 200_000 * 100, 1) if recent_ctx else 0
            )
        except (ValueError, TypeError):
            ctx_pct = 0

        agents.append(
            {
                "id": session_id,
                "name": label,
                "status": status,
                "currentTool": current_tool if is_running else None,
                "toolStartedAt": tool_started_at if is_running else None,
                "sessionId": session_id,
                "projectName": project_name or "",
                "startedAt": started_at or last_active,
                "lastActivityAt": last_active,
                "parentId": None,
                "isSubagent": False,
                "turnCount": int(turn_count or 0),
                "costUsd": round(
                    cost_usd(model or "", int(total_in or 0), int(total_out or 0)), 4
                ),
                "errorCount": int(error_count or 0),
                "errorStreak": streaks.get(session_id, 0),
                "ctxPct": ctx_pct,
                "model": model or "",
                "cwd": cwd or "",
                "context": context,
            }
        )

    return jsonify({"agents": agents})
