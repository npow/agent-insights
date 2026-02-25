"""Convergence/drift/thrash composite scores and trajectory classification."""

from .config import (
    CONVERGENCE_WEIGHTS,
    DRIFT_WEIGHTS,
    THRASH_WEIGHTS,
    TRAJECTORY_THRESHOLDS,
)
from .db import get_conn, get_writer


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def compute_scores():
    """Compute convergence/drift/thrash scores for all sessions."""
    conn = get_writer()

    try:
        rows = conn.execute("""
            SELECT
                s.session_id, s.duration_seconds, s.tool_error_count, s.tool_use_count,
                f.prompt_length_trend, f.decision_marker_count, f.correction_rate,
                f.response_length_cv, f.has_pr_link,
                f.topic_keyword_entropy, f.sidechain_ratio, f.branch_switch_count,
                f.prompt_length_oscillation, f.api_error_count, f.rephrasing_count,
                f.abandoned, s.user_prompt_count
            FROM sessions s
            JOIN session_features f ON s.session_id = f.session_id
        """).fetchall()

        for row in rows:
            (
                session_id,
                duration,
                tool_errors,
                tool_uses,
                prompt_trend,
                decisions,
                correction_rate,
                response_cv,
                has_pr,
                keyword_entropy,
                sidechain_ratio,
                branch_switches,
                oscillation,
                api_errors,
                rephrasing,
                abandoned,
                prompt_count,
            ) = row

            # --- Convergence ---
        # prompt_length_decrease: negative trend is good
            c_prompt = _clamp(max(0, -prompt_trend) / 0.5)
            # decision_markers: more is better (normalize by prompt count)
            c_decisions = _clamp(decisions / max(prompt_count, 1) / 0.5)
            # low correction rate
            c_correction = _clamp(1.0 - correction_rate * 3)
            # low tool error rate
            error_rate = tool_errors / max(tool_uses, 1)
            c_tool_error = _clamp(1.0 - error_rate * 5)
            # has PR
            c_pr = 1.0 if has_pr else 0.0
            # stable response length (low CV)
            c_stable = _clamp(1.0 - response_cv)

            w = CONVERGENCE_WEIGHTS
            convergence = (
                w["prompt_length_decrease"] * c_prompt
                + w["decision_markers"] * c_decisions
                + w["low_correction_rate"] * c_correction
                + w["low_tool_error_rate"] * c_tool_error
                + w["has_pr"] * c_pr
                + w["stable_response_length"] * c_stable
            )

            # --- Drift ---
            d_entropy = _clamp(keyword_entropy / 0.7)
            d_prompt_inc = _clamp(max(0, prompt_trend) / 0.5)
            d_branch = _clamp(branch_switches / 3)
            d_sidechain = _clamp(sidechain_ratio / 0.3)
            d_no_decisions = _clamp(1.0 - decisions / max(prompt_count, 1) / 0.3)
            d_long = _clamp((duration - 1800) / 3600) if duration > 1800 else 0.0

            w = DRIFT_WEIGHTS
            drift = (
                w["keyword_entropy"] * d_entropy
                + w["increasing_prompt_length"] * d_prompt_inc
                + w["branch_switches"] * d_branch
                + w["sidechain_ratio"] * d_sidechain
                + w["no_decisions"] * d_no_decisions
                + w["long_session"] * d_long
            )

            # --- Thrash ---
            t_correction = _clamp(correction_rate * 3)
            t_tool_error = _clamp(error_rate * 5)
            t_rephrasing = _clamp(rephrasing / max(prompt_count, 1) / 0.3)
            t_oscillation = _clamp(oscillation)
            t_api_errors = _clamp(api_errors / max(prompt_count, 1))

            w = THRASH_WEIGHTS
            thrash = (
                w["correction_rate"] * t_correction
                + w["tool_error_rate"] * t_tool_error
                + w["rephrasing"] * t_rephrasing
                + w["oscillating_lengths"] * t_oscillation
                + w["api_errors"] * t_api_errors
            )

            convergence = _clamp(convergence)
            drift = _clamp(drift)
            thrash = _clamp(thrash)

            # Trajectory classification
            trajectory = classify_trajectory(convergence, drift, thrash, abandoned)

            conn.execute(
                """
                UPDATE sessions SET
                    convergence_score = ?, drift_score = ?, thrash_score = ?,
                    trajectory = ?
                WHERE session_id = ?
            """,
                [convergence, drift, thrash, trajectory, session_id],
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(rows)


def classify_trajectory(
    convergence: float, drift: float, thrash: float, abandoned: bool
) -> str:
    if abandoned:
        return "abandoned"
    t = TRAJECTORY_THRESHOLDS
    if (
        convergence >= t["converged"]["convergence_min"]
        and drift <= t["converged"]["drift_max"]
        and thrash <= t["converged"]["thrash_max"]
    ):
        return "converged"
    if (
        drift >= t["drifted"]["drift_min"]
        and convergence <= t["drifted"]["convergence_max"]
    ):
        return "drifted"
    if (
        thrash >= t["thrashed"]["thrash_min"]
        and convergence <= t["thrashed"]["convergence_max"]
    ):
        return "thrashed"
    if convergence >= 0.4:
        return "mixed"
    return "unknown"
