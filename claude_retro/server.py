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
            "top_project_pct": round(top_proj[1] / total_sessions, 2) if top_proj and total_sessions else 0,
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

    conditions = ["s.turn_count >= 1", "s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'"]
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
        SELECT tool_name, use_count, error_count
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
            {"tool_name": t[0], "use_count": t[1], "error_count": t[2]} for t in tools
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

    MAX = 400
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
                turn["text"] = content[:MAX]
            elif isinstance(content, list):
                text_parts = []
                for block in content:
                    btype = block.get("type", "")
                    if btype == "text":
                        text_parts.append(block.get("text", ""))
                    elif btype == "tool_use":
                        inp = block.get("input", {})
                        inp_str = json.dumps(inp, ensure_ascii=False)
                        turn["tools"].append({
                            "name": block.get("name", ""),
                            "id": block.get("id", ""),
                            "input_preview": inp_str[:MAX],
                        })
                    elif btype == "tool_result":
                        turn["is_tool_result"] = True
                        turn["is_error"] = bool(block.get("is_error", False))
                        turn["tool_id"] = block.get("tool_use_id", "")
                        rc = block.get("content", "")
                        if isinstance(rc, list):
                            rc = " ".join(
                                b.get("text", "") for b in rc if b.get("type") == "text"
                            )
                        turn["result_preview"] = str(rc)[:MAX] if rc else None
                if text_parts:
                    turn["text"] = "\n".join(text_parts)[:MAX]

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

        rows = conn.execute(f"""
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
        """, params).fetchall()
    except Exception:
        # FTS query failed — fall back to LIKE search
        like_q = f"%{q}%"
        params = [like_q, like_q]
        project_filter = ""
        if project:
            project_filter = "AND s.project_name = ?"
            params.append(project)
        params.append(limit)

        rows = conn.execute(f"""
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
        """, params).fetchall()

    results = []
    seen_sessions = set()
    for row in rows:
        sid = row[0]
        # Deduplicate by session (show max 2 results per session)
        count = sum(1 for r in results if r["session_id"] == sid)
        if count >= 2:
            continue
        results.append({
            "session_id": sid,
            "entry_type": row[1],
            "project": row[2],
            "first_prompt": (row[3] or "")[:80],
            "started_at": _serialize(row[4]),
            "snippet": row[5],
            "timestamp": _serialize(row[6]),
        })

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
            SUM(CASE WHEN j.misalignment_count > 0 THEN 1 ELSE 0 END) as sessions_with_misalignment
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
            short_project = (project or "").replace("-Users-npow-code-", "").replace("-Users-npow-", "")
            example_sessions.append({
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
            })

        results.append({
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
        })

    return jsonify({"dimensions": results})


@app.route("/api/synthesis")
def api_synthesis():
    """Return the cross-session synthesis report."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM synthesis WHERE id = 1").fetchone()
    if not row:
        return jsonify({"synthesis": None})

    cols = [d[0] for d in conn.execute("SELECT * FROM synthesis WHERE id = 1").description]
    result = _row_to_dict(row, cols)

    # Parse JSON fields
    for field in ("at_a_glance", "top_wins", "top_friction", "claude_md_additions",
                  "workflow_prompts", "features_to_try"):
        if result.get(field) and isinstance(result[field], str):
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, ValueError):
                pass

    return jsonify({"synthesis": result})


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

    return jsonify({
        "narrative": {
            "narrative": row[0],
            "what_worked": row[1],
            "what_failed": row[2],
            "user_quote": row[3],
            "claude_md_suggestion": row[4],
            "claude_md_rationale": row[5],
            "prompt_summary": row[6],
        }
    })


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
                synthesis_suggestions.append({
                    "rule": a.get("rule", ""),
                    "rationale": a.get("rationale", ""),
                    "evidence": a.get("evidence", ""),
                    "source": "synthesis",
                })
        except (json.JSONDecodeError, ValueError):
            pass

    session_suggestions = []
    for r in rows:
        session_suggestions.append({
            "session_id": r[0],
            "rule": r[1],
            "rationale": r[2],
            "prompt_summary": r[3],
            "project_name": r[4],
            "started_at": _serialize(r[5]),
            "source": "session",
        })

    return jsonify({
        "synthesis_suggestions": synthesis_suggestions,
        "session_suggestions": session_suggestions,
    })


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
        highlights.append({
            "type": "most_productive",
            "label": "Most Productive",
            "session_id": row[0], "productivity": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_worked": row[8],
        })

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
        highlights.append({
            "type": "most_wasteful",
            "label": "Most Wasteful",
            "session_id": row[0], "productivity": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_failed": row[8], "misalignments": row[9],
        })

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
        highlights.append({
            "type": "most_misaligned",
            "label": "Most Misaligned",
            "session_id": row[0], "misalignments": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_failed": row[8],
        })

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
        highlights.append({
            "type": "best_prompt",
            "label": "Best Prompt",
            "session_id": row[0], "clarity": row[1], "completeness": row[2],
            "outcome": row[3], "narrative": row[4], "prompt_summary": row[5],
            "project": row[6], "started_at": _serialize(row[7]),
            "what_worked": row[8],
        })

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
        highlights.append({
            "type": "longest_success",
            "label": "Longest Success",
            "session_id": row[0], "duration": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "turns": row[7], "what_worked": row[8],
        })

    return jsonify({"highlights": highlights[:5]})


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
            "calendar": [
                {"date": str(r[0]), "count": r[1]}
                for r in rows
            ],
        }
    )
