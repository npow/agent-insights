"""Populate sessions table from raw_entries."""

from .db import get_writer


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
                session_id, project_name, agent_type, started_at, ended_at, duration_seconds,
                user_prompt_count, assistant_msg_count, tool_use_count, tool_error_count,
                turn_count, first_prompt
            )
            SELECT
                agg.session_id,
                agg.project_name,
                agg.agent_type,
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
                    COALESCE(MAX(CASE WHEN agent_type IS NOT NULL AND agent_type != '' AND agent_type != 'unknown' THEN agent_type END), 'unknown') as agent_type,
                    MIN(timestamp_utc) as started_at,
                    MAX(timestamp_utc) as ended_at,
                    CAST((julianday(MAX(timestamp_utc)) - julianday(MIN(timestamp_utc))) * 86400 AS INTEGER) as duration_seconds,
                    SUM(CASE WHEN entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0 THEN 1 ELSE 0 END) as user_prompt_count,
                    SUM(CASE WHEN entry_type = 'assistant' THEN 1 ELSE 0 END) as assistant_msg_count,
                    COALESCE(SUM(CASE WHEN tool_names IS NOT NULL THEN length(tool_names) - length(REPLACE(tool_names, ',', '')) + 1 ELSE 0 END), 0) as tool_use_count,
                    SUM(CASE WHEN tool_result_error = 1 THEN 1 ELSE 0 END) as tool_error_count,
                    -- For Claude/Codex: count turn_duration system entries.
                    -- For agents without turn_duration (e.g. Windsurf): fall back to user prompt count.
                    CASE
                        WHEN SUM(CASE WHEN entry_type = 'system' AND system_subtype = 'turn_duration' THEN 1 ELSE 0 END) > 0
                        THEN SUM(CASE WHEN entry_type = 'system' AND system_subtype = 'turn_duration' THEN 1 ELSE 0 END)
                        ELSE SUM(CASE WHEN entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0 THEN 1 ELSE 0 END)
                    END as turn_count
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
    """Aggregate per-tool call counts, error counts, and timing per session.

    Uses the primary tool (first in tool_names array) per assistant entry for
    duration attribution. Duration = tool_result.timestamp - assistant.timestamp.
    """
    conn = get_writer()

    try:
        conn.execute("DELETE FROM session_tool_usage")
        conn.execute("""
            INSERT INTO session_tool_usage
                (session_id, tool_name, use_count, error_count, total_duration_ms, avg_duration_ms)
            SELECT
                r.session_id,
                json_extract(r.tool_names, '$[0]')                              AS tool_name,
                COUNT(*)                                                         AS use_count,
                SUM(COALESCE(tr.tool_result_error, 0))                          AS error_count,
                COALESCE(SUM(
                    CASE WHEN tr.timestamp_utc IS NOT NULL THEN
                        CAST((julianday(tr.timestamp_utc) - julianday(r.timestamp_utc)) * 86400000 AS INTEGER)
                    ELSE 0 END
                ), 0)                                                            AS total_duration_ms,
                AVG(
                    CASE WHEN tr.timestamp_utc IS NOT NULL THEN
                        CAST((julianday(tr.timestamp_utc) - julianday(r.timestamp_utc)) * 86400000 AS INTEGER)
                    ELSE NULL END
                )                                                                AS avg_duration_ms
            FROM raw_entries r
            LEFT JOIN raw_entries tr
                   ON tr.parent_uuid = r.entry_id
                  AND tr.is_tool_result = 1
            WHERE r.entry_type = 'assistant'
              AND r.tool_names IS NOT NULL
              AND r.tool_names != '[]'
              AND r.session_id IS NOT NULL
              AND json_extract(r.tool_names, '$[0]') IS NOT NULL
            GROUP BY r.session_id, json_extract(r.tool_names, '$[0]')
        """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    count = conn.execute("SELECT COUNT(*) FROM session_tool_usage").fetchone()[0]
    return count
