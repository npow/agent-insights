"""Rolling 14/60 session window baselines."""

from .config import BASELINE_WINDOWS
from .db import get_writer


def compute_baselines():
    """Compute rolling window baselines."""
    conn = get_writer()

    try:
        conn.execute("DELETE FROM baselines")

        for window_size in BASELINE_WINDOWS:
            result = conn.execute(
                """
                SELECT
                    AVG(convergence_score),
                    AVG(drift_score),
                    AVG(thrash_score),
                    AVG(duration_seconds),
                    AVG(turn_count),
                    AVG(tool_error_count),
                    COUNT(*)
                FROM (
                    SELECT * FROM sessions
                    ORDER BY started_at DESC
                    LIMIT ?
                )
            """,
                [window_size],
            ).fetchone()

            if result and result[6] > 0:
                # Compute avg correction rate from features
                avg_correction = (
                    conn.execute(
                        """
                    SELECT AVG(f.correction_rate) FROM session_features f
                    JOIN (
                        SELECT session_id FROM sessions
                        ORDER BY started_at DESC LIMIT ?
                    ) s ON f.session_id = s.session_id
                """,
                        [window_size],
                    ).fetchone()[0]
                    or 0
                )

                conn.execute(
                    """
                        INSERT INTO baselines (id, window_size, computed_at,
                            avg_convergence, avg_drift, avg_thrash,
                            avg_duration, avg_turns, avg_tool_errors,
                            avg_correction_rate, session_count)
                        VALUES (?, ?, current_timestamp, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        window_size,
                        window_size,
                        result[0],
                        result[1],
                        result[2],
                        result[3],
                        result[4],
                        result[5],
                        avg_correction,
                        result[6],
                    ],
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(BASELINE_WINDOWS)
