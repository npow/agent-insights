"""Populate sessions table from raw_entries."""

from .db import get_conn, get_writer


def build_sessions():
    """Aggregate raw_entries into session-level rows.

    Uses BEGIN/COMMIT so the DELETE only takes effect if the INSERT succeeds,
    preventing data loss on query errors.
    """
    from .db import get_writer

    conn = get_writer()

    try:
        conn.execute("DELETE FROM sessions")

        conn.execute("""
            INSERT OR REPLACE INTO sessions (
                session_id, project_name, started_at, ended_at, duration_seconds,
                user_prompt_count, assistant_msg_count, tool_use_count, tool_error_count,
                turn_count, first_prompt
            )
            SELECT
                agg.session_id,
                agg.project_name,
                agg.started_at,
                agg.ended_at,
                agg.duration_seconds,
                agg.user_prompt_count,
                agg.assistant_msg_count,
                agg.tool_use_count,
                agg.tool_error_count,
                agg.turn_count,
                fp.user_text as first_prompt
            FROM (
                SELECT
                    session_id,
                    MAX(project_name) as project_name,
                    MIN(timestamp_utc) as started_at,
                    MAX(timestamp_utc) as ended_at,
                    CAST((julianday(MAX(timestamp_utc)) - julianday(MIN(timestamp_utc))) * 86400 AS INTEGER) as duration_seconds,
                    SUM(CASE WHEN entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0 THEN 1 ELSE 0 END) as user_prompt_count,
                    SUM(CASE WHEN entry_type = 'assistant' THEN 1 ELSE 0 END) as assistant_msg_count,
                    COALESCE(SUM(CASE WHEN tool_names IS NOT NULL THEN length(tool_names) - length(REPLACE(tool_names, ',', '')) + 1 ELSE 0 END), 0) as tool_use_count,
                    SUM(CASE WHEN tool_result_error = 1 THEN 1 ELSE 0 END) as tool_error_count,
                    SUM(CASE WHEN entry_type = 'system' AND system_subtype = 'turn_duration' THEN 1 ELSE 0 END) as turn_count
                FROM raw_entries
                WHERE session_id IS NOT NULL
                GROUP BY session_id
                HAVING COUNT(*) >= 2
            ) agg
            LEFT JOIN (
                SELECT session_id, user_text
                FROM raw_entries
                WHERE entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0
                  AND (session_id, timestamp_utc) IN (
                      SELECT session_id, MIN(timestamp_utc)
                      FROM raw_entries
                      WHERE entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0
                      GROUP BY session_id
                  )
            ) fp ON agg.session_id = fp.session_id
        """)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    return count


def build_tool_usage():
    """Aggregate tool usage per session.

    Note: This function is currently simplified - it doesn't parse tool_names
    since SQLite doesn't have UNNEST. Tool usage stats are computed in tool_use_count
    in the sessions table instead.
    """
    conn = get_writer()

    try:
        conn.execute("DELETE FROM session_tool_usage")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    # Return 0 since we're not populating this table for now
    # The tool_use_count in sessions table provides overall tool usage stats
    return 0
