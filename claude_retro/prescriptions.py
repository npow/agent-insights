"""Generate actionable prescriptions from session data."""

import json

from .db import get_conn, get_writer


def generate_prescriptions():
    """Generate prescriptive insights based on session data."""
    conn = get_writer()

    try:
        # Clear old non-dismissed prescriptions
        conn.execute("DELETE FROM prescriptions WHERE dismissed = FALSE")

        count = 0
        count += _time_of_day_insight(conn)
        count += _first_prompt_quality_insight(conn)
        count += _session_length_insight(conn)
        count += _project_flags(conn)
        count += _trend_insight(conn)
        count += _tool_error_hotspot(conn)
        count += _judgment_prompt_quality_insight(conn)
        count += _judgment_misalignment_insight(conn)
        count += _judgment_underspec_patterns(conn)
        count += _skill_gap_prescriptions(conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return count


def generate_actions():
    """Generate action items for the /api/actions endpoint.

    Returns a list of action dicts without touching the prescriptions table.
    """
    conn = get_writer()
    actions = []
    actions += _action_time_of_day(conn)
    actions += _action_first_prompt_quality(conn)
    actions += _action_session_length(conn)
    actions += _action_project_flags(conn)
    actions += _action_trend(conn)
    actions += _action_tool_error_hotspot(conn)
    actions += _action_judgment_prompt_quality(conn)
    actions += _action_judgment_misalignment(conn)
    actions += _action_judgment_underspec_patterns(conn)
    actions += _action_prompt_length_correlation(conn)
    actions += _action_correction_impact(conn)
    actions += _action_tool_focus(conn)
    actions += _action_skill_gaps(conn)
    return actions


# ---------------------------------------------------------------------------
# Prescription generators (write to DB)
# ---------------------------------------------------------------------------


def _time_of_day_insight(conn) -> int:
    result = conn.execute("""
        SELECT
            CASE
                WHEN f.hour_of_day BETWEEN 6 AND 11 THEN 'morning'
                WHEN f.hour_of_day BETWEEN 12 AND 17 THEN 'afternoon'
                WHEN f.hour_of_day BETWEEN 18 AND 22 THEN 'evening'
                ELSE 'night'
            END as period,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY period
        HAVING n >= 5
        ORDER BY avg_conv DESC
    """).fetchall()

    if len(result) >= 2:
        best = result[0]
        worst = result[-1]
        delta = best[1] - worst[1]
        if delta > 0.05:
            conn.execute(
                """
                INSERT INTO prescriptions (category, title, description, evidence, confidence)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    "scheduling",
                    f"Your {best[0]} sessions converge {delta:.0%} better",
                    f"Schedule complex work in the {best[0]}. "
                    f"{best[0].title()} sessions converge at {best[1]:.0%} vs {worst[1]:.0%} for {worst[0]}.",
                    f"Based on {best[2]} {best[0]} vs {worst[2]} {worst[0]} sessions.",
                    min(0.9, 0.5 + delta),
                ],
            )
            return 1
    return 0


def _first_prompt_quality_insight(conn) -> int:
    result = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero_corrections' ELSE 'has_corrections' END as bucket,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY bucket
        HAVING n >= 3
    """).fetchall()

    buckets = {r[0]: (r[1], r[2]) for r in result}
    if "zero_corrections" in buckets and "has_corrections" in buckets:
        zero_conv = buckets["zero_corrections"][0]
        has_conv = buckets["has_corrections"][0]
        delta = zero_conv - has_conv
        if delta > 0.08:
            conn.execute(
                """
                INSERT INTO prescriptions (category, title, description, evidence, confidence)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    "prompt_quality",
                    "Zero-correction sessions vastly outperform",
                    f"Sessions with no corrections converge at {zero_conv:.0%} vs {has_conv:.0%}. "
                    f"Invest more time crafting your first prompt to avoid back-and-forth.",
                    f"Based on {buckets['zero_corrections'][1]} zero-correction vs "
                    f"{buckets['has_corrections'][1]} sessions with corrections.",
                    min(0.92, 0.5 + delta),
                ],
            )
            return 1
    return 0


def _session_length_insight(conn) -> int:
    result = conn.execute("""
        SELECT
            CASE
                WHEN s.duration_seconds < 900 THEN 'short (<15m)'
                WHEN s.duration_seconds < 1800 THEN 'medium (15-30m)'
                WHEN s.duration_seconds < 3600 THEN 'long (30-60m)'
                ELSE 'marathon (>1h)'
            END as bucket,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        GROUP BY bucket
        HAVING n >= 3
        ORDER BY avg_conv DESC
    """).fetchall()

    if len(result) >= 2:
        best = result[0]
        worst = result[-1]
        delta = best[1] - worst[1]
        if delta > 0.05:
            conn.execute(
                """
                INSERT INTO prescriptions (category, title, description, evidence, confidence)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    "session_length",
                    f"{best[0]} sessions have highest convergence",
                    f"Sessions under 15 minutes converge at {best[1]:.0%} vs {worst[1]:.0%} for "
                    f"{worst[0]} sessions. Break complex work into smaller chunks.",
                    f"Based on {best[2]} {best[0]} vs {worst[2]} {worst[0]} sessions.",
                    min(0.85, 0.5 + delta),
                ],
            )
            return 1
    return 0


def _project_flags(conn) -> int:
    _filter = """
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    # Use judgment-based metrics when available, fall back to basic metrics
    avgs = conn.execute(f"""
        SELECT
            AVG(j.productivity_ratio) as avg_prod,
            AVG(j.misalignment_count) as avg_mis,
            AVG(s.tool_error_count) as avg_errors
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        {_filter}
    """).fetchone()
    if not avgs:
        return 0

    avg_prod = avgs[0]
    avg_mis = avgs[1] or 0
    avg_errors = avgs[2] or 0

    projects = conn.execute(f"""
        SELECT
            s.project_name,
            COUNT(*) as n,
            AVG(j.productivity_ratio) as avg_prod,
            AVG(j.misalignment_count) as avg_mis,
            SUM(s.tool_error_count) as total_errors,
            AVG(s.tool_error_count) as avg_errors,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN j.outcome IS NOT NULL THEN 1 ELSE 0 END), 0) as completion_rate
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        {_filter}
        GROUP BY s.project_name
        HAVING n >= 3
        ORDER BY avg_prod ASC NULLS LAST
    """).fetchall()

    count = 0
    for p in projects:
        name, n, prod, mis, total_errors, p_avg_errors, comp_rate = p
        short_name = name.replace("-Users-npow-code-", "").replace("-Users-npow-", "")

        problems = []
        advice = []
        if prod is not None and avg_prod is not None and prod < avg_prod * 0.8:
            problems.append(f"productivity {prod:.0%} vs {avg_prod:.0%} avg")
            advice.append("break tasks into smaller, well-scoped prompts")
        if mis is not None and avg_mis is not None and mis > avg_mis * 1.5 and mis >= 2:
            problems.append(f"{mis:.1f} misalignments/session vs {avg_mis:.1f} avg")
            advice.append("add explicit constraints and expected behavior to your prompts")
        if p_avg_errors > avg_errors * 1.5 and total_errors >= 3:
            problems.append(
                f"{total_errors:.0f} tool errors ({p_avg_errors:.1f}/session vs {avg_errors:.1f} avg)"
            )
            advice.append("check if there's a recurring environment or config issue")

        if not problems:
            continue

        # Use ||SPLIT|| delimiter so frontend can render diagnosis vs advice separately
        diagnosis = "; ".join(problems).capitalize()
        if comp_rate is not None:
            diagnosis += f". Completion rate: {comp_rate:.0%}"
        action_text = ". ".join(a.capitalize() for a in advice)

        conn.execute(
            """
            INSERT INTO prescriptions (category, title, description, evidence, confidence)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                "project_health",
                f"Improve '{short_name}' sessions",
                f"{diagnosis}||SPLIT||{action_text}",
                f"project:{name}:{n}",
                0.8,
            ],
        )
        count += 1
        if count >= 3:
            break
    return count


def _trend_insight(conn) -> int:
    rows = conn.execute("""
        SELECT convergence_score, thrash_score
        FROM sessions
        ORDER BY started_at DESC
        LIMIT 40
    """).fetchall()

    if len(rows) < 30:
        return 0

    recent = rows[:20]
    previous = rows[20:40]

    r_conv = sum(r[0] for r in recent) / len(recent)
    p_conv = sum(r[0] for r in previous) / len(previous)
    r_thrash = sum(r[1] for r in recent) / len(recent)
    p_thrash = sum(r[1] for r in previous) / len(previous)

    conv_delta = r_conv - p_conv
    thrash_delta = r_thrash - p_thrash

    parts = []
    if abs(conv_delta) > 0.03:
        direction = "up" if conv_delta > 0 else "down"
        parts.append(f"convergence {direction} {abs(conv_delta):.0%}")
    if abs(thrash_delta) > 0.03:
        direction = "up" if thrash_delta > 0 else "down"
        parts.append(f"thrash {direction} {abs(thrash_delta):.0%}")

    if not parts:
        return 0

    title = "Recent trend: " + ", ".join(parts)
    is_positive = conv_delta > 0.03 and thrash_delta <= 0.03

    conn.execute(
        """
        INSERT INTO prescriptions (category, title, description, evidence, confidence)
        VALUES (?, ?, ?, ?, ?)
    """,
        [
            "trend",
            title,
            f"Last 20 sessions: convergence {r_conv:.0%} (was {p_conv:.0%}), "
            f"thrash {r_thrash:.0%} (was {p_thrash:.0%}). "
            + ("Keep it up!" if is_positive else "Watch the trend."),
            "Comparing last 20 sessions vs previous 20.",
            0.75,
        ],
    )
    return 1


def _tool_error_hotspot(conn) -> int:
    total_errors = conn.execute("""
        SELECT SUM(error_count) FROM session_tool_usage
    """).fetchone()[0]

    if not total_errors or total_errors < 5:
        return 0

    top = conn.execute("""
        SELECT tool_name, SUM(error_count) as errors
        FROM session_tool_usage
        GROUP BY tool_name
        HAVING errors > 0
        ORDER BY errors DESC
        LIMIT 1
    """).fetchone()

    if not top:
        return 0

    tool_name, tool_errors = top
    ratio = tool_errors / total_errors

    if ratio > 0.4:
        conn.execute(
            """
            INSERT INTO prescriptions (category, title, description, evidence, confidence)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                "tool_errors",
                f"'{tool_name}' accounts for {ratio:.0%} of all tool errors",
                f"The {tool_name} tool generated {tool_errors} of {total_errors} total errors. "
                f"Check if you're hitting a recurring issue with this tool.",
                "Based on all session tool usage data.",
                min(0.85, 0.5 + ratio * 0.5),
            ],
        )
        return 1
    return 0


# ---------------------------------------------------------------------------
# Action generators (return dicts, no DB writes)
# ---------------------------------------------------------------------------


def _action_time_of_day(conn):
    result = conn.execute("""
        SELECT
            CASE
                WHEN f.hour_of_day BETWEEN 6 AND 11 THEN 'morning'
                WHEN f.hour_of_day BETWEEN 12 AND 17 THEN 'afternoon'
                WHEN f.hour_of_day BETWEEN 18 AND 22 THEN 'evening'
                ELSE 'night'
            END as period,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY period
        HAVING n >= 5
        ORDER BY avg_conv DESC
    """).fetchall()

    if len(result) >= 2:
        best = result[0]
        worst = result[-1]
        delta = best[1] - worst[1]
        if delta > 0.05:
            return [
                {
                    "type": "tip",
                    "title": f"Your {best[0]} sessions converge {delta:.0%} better",
                    "body": f"Schedule complex work in the {best[0]}. "
                    f"{best[0].title()} convergence: {best[1]:.0%} vs {worst[0]}: {worst[1]:.0%}.",
                    "evidence": f"{best[2]} {best[0]} vs {worst[2]} {worst[0]} sessions",
                }
            ]
    return []


def _action_first_prompt_quality(conn):
    result = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero' ELSE 'has' END as bucket,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY bucket
        HAVING n >= 3
    """).fetchall()

    buckets = {r[0]: (r[1], r[2]) for r in result}
    if "zero" in buckets and "has" in buckets:
        zero_conv = buckets["zero"][0]
        has_conv = buckets["has"][0]
        delta = zero_conv - has_conv
        if delta > 0.08:
            return [
                {
                    "type": "tip",
                    "title": "Invest in your first prompt",
                    "body": f"Sessions with zero corrections converge at {zero_conv:.0%} vs "
                    f"{has_conv:.0%} with corrections. Spend more time upfront.",
                    "evidence": f"{buckets['zero'][1]} zero-correction vs {buckets['has'][1]} with corrections",
                }
            ]
    return []


def _action_session_length(conn):
    result = conn.execute("""
        SELECT
            CASE
                WHEN s.duration_seconds < 900 THEN 'short (<15m)'
                WHEN s.duration_seconds < 1800 THEN 'medium (15-30m)'
                WHEN s.duration_seconds < 3600 THEN 'long (30-60m)'
                ELSE 'marathon (>1h)'
            END as bucket,
            AVG(s.convergence_score) as avg_conv,
            COUNT(*) as n
        FROM sessions s
        GROUP BY bucket
        HAVING n >= 3
        ORDER BY avg_conv DESC
    """).fetchall()

    if len(result) >= 2:
        best = result[0]
        worst = result[-1]
        delta = best[1] - worst[1]
        if delta > 0.05:
            return [
                {
                    "type": "tip",
                    "title": f"{best[0]} sessions converge best",
                    "body": f"Convergence: {best[1]:.0%} for {best[0]} vs {worst[1]:.0%} for {worst[0]}. "
                    f"Break work into smaller chunks.",
                    "evidence": f"{best[2]} vs {worst[2]} sessions",
                }
            ]
    return []


def _action_project_flags(conn):
    avgs = conn.execute("""
        SELECT
            AVG(j.productivity_ratio),
            AVG(j.misalignment_count),
            AVG(s.tool_error_count)
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
    """).fetchone()
    if not avgs:
        return []

    avg_prod, avg_mis, avg_errors = avgs[0], avgs[1] or 0, avgs[2] or 0

    projects = conn.execute("""
        SELECT
            s.project_name, COUNT(*) as n,
            AVG(j.productivity_ratio) as avg_prod,
            AVG(j.misalignment_count) as avg_mis,
            SUM(s.tool_error_count) as total_errors,
            AVG(s.tool_error_count) as avg_errors
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY s.project_name
        HAVING n >= 3
        ORDER BY avg_prod ASC NULLS LAST
    """).fetchall()

    actions = []
    for p in projects:
        name, n, prod, mis, total_errors, p_avg_errors = p
        short = name.replace("-Users-npow-code-", "").replace("-Users-npow-", "")

        reasons = []
        if prod is not None and avg_prod is not None and prod < avg_prod * 0.8:
            reasons.append(f"productivity {prod:.0%} vs {avg_prod:.0%} avg")
        if mis is not None and avg_mis is not None and mis > avg_mis * 1.5 and mis >= 2:
            reasons.append(f"{mis:.1f} misalignments/session")
        if p_avg_errors > avg_errors * 1.5 and total_errors >= 3:
            reasons.append(f"{total_errors:.0f} tool errors")

        if not reasons:
            continue

        actions.append(
            {
                "type": "warning",
                "title": f"Project '{short}' struggling",
                "body": f"{', '.join(reasons).capitalize()}. "
                f"Consider reviewing your approach for this project.",
                "evidence": f"project:{name}:{n}",
            }
        )
        if len(actions) >= 2:
            break
    return actions


def _action_trend(conn):
    rows = conn.execute("""
        SELECT convergence_score, thrash_score
        FROM sessions ORDER BY started_at DESC LIMIT 40
    """).fetchall()

    if len(rows) < 30:
        return []

    recent = rows[:20]
    previous = rows[20:40]

    r_conv = sum(r[0] for r in recent) / len(recent)
    p_conv = sum(r[0] for r in previous) / len(previous)
    r_thrash = sum(r[1] for r in recent) / len(recent)
    p_thrash = sum(r[1] for r in previous) / len(previous)

    conv_delta = r_conv - p_conv
    thrash_delta = r_thrash - p_thrash

    parts = []
    if abs(conv_delta) > 0.03:
        parts.append(
            f"Convergence {'up' if conv_delta > 0 else 'down'} {abs(conv_delta):.0%}"
        )
    if abs(thrash_delta) > 0.03:
        parts.append(
            f"thrash {'up' if thrash_delta > 0 else 'down'} {abs(thrash_delta):.0%}"
        )

    if not parts:
        return []

    is_good = conv_delta > 0.03 and thrash_delta <= 0.03
    is_bad = conv_delta < -0.03 or thrash_delta > 0.05

    return [
        {
            "type": "positive" if is_good else ("warning" if is_bad else "tip"),
            "title": ", ".join(parts) + " over last 20 sessions",
            "body": f"Recent: convergence {r_conv:.0%}, thrash {r_thrash:.0%}. "
            f"Previous 20: convergence {p_conv:.0%}, thrash {p_thrash:.0%}.",
            "evidence": "Comparing last 20 vs previous 20 sessions",
        }
    ]


def _action_tool_error_hotspot(conn):
    total_errors = conn.execute(
        "SELECT SUM(error_count) FROM session_tool_usage"
    ).fetchone()[0]

    if not total_errors or total_errors < 5:
        return []

    top = conn.execute("""
        SELECT tool_name, SUM(error_count) as errors
        FROM session_tool_usage
        GROUP BY tool_name HAVING errors > 0
        ORDER BY errors DESC LIMIT 1
    """).fetchone()

    if not top:
        return []

    tool_name, tool_errors = top
    ratio = tool_errors / total_errors

    if ratio > 0.4:
        return [
            {
                "type": "warning",
                "title": f"'{tool_name}' causes {ratio:.0%} of tool errors",
                "body": f"{tool_errors} of {total_errors} total errors come from {tool_name}. "
                f"Check for a recurring issue.",
                "evidence": "Across all sessions",
            }
        ]
    return []


# ---------------------------------------------------------------------------
# Judgment-based prescription generators (write to DB)
# ---------------------------------------------------------------------------


def _judgment_prompt_quality_insight(conn) -> int:
    """Flag low prompt clarity based on LLM judgments."""
    result = conn.execute("""
        SELECT AVG(prompt_clarity), AVG(prompt_completeness), COUNT(*)
        FROM session_judgments
    """).fetchone()
    if not result or not result[2] or result[2] < 5:
        return 0

    avg_clarity, avg_completeness, n = result
    if avg_clarity < 0.6:
        conn.execute(
            """
            INSERT INTO prescriptions (category, title, description, evidence, confidence)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                "prompt_quality",
                f"AI analysis: prompt clarity averaging {avg_clarity:.0%}",
                f"LLM analysis of your sessions finds average prompt clarity at {avg_clarity:.0%} "
                f"and completeness at {avg_completeness:.0%}. Consider including more context, "
                f"specific requirements, and expected outcomes in your initial prompts.",
                f"Based on AI analysis of {n} sessions.",
                min(0.85, 0.5 + (0.6 - avg_clarity)),
            ],
        )
        return 1
    return 0


def _judgment_misalignment_insight(conn) -> int:
    """Warn about high misalignment rate per project."""
    results = conn.execute("""
        SELECT s.project_name,
               COUNT(*) as n,
               SUM(CASE WHEN j.misalignment_count > 0 THEN 1 ELSE 0 END) as misaligned
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY s.project_name
        HAVING n >= 3
    """).fetchall()

    count = 0
    for project_name, n, misaligned in results:
        rate = misaligned / n
        if rate > 0.3:
            short = project_name.replace("-Users-npow-code-", "").replace(
                "-Users-npow-", ""
            )
            conn.execute(
                """
                INSERT INTO prescriptions (category, title, description, evidence, confidence)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    "project_health",
                    f"AI analysis: '{short}' has {rate:.0%} misalignment rate",
                    f"LLM analysis found that {misaligned} of {n} sessions in '{short}' "
                    f"had misalignments where Claude went off-track and needed correction. "
                    f"Consider writing more detailed prompts for this project.",
                    f"Based on AI analysis of {n} sessions.",
                    min(0.85, 0.5 + rate * 0.5),
                ],
            )
            count += 1
            if count >= 2:
                break
    return count


def _judgment_underspec_patterns(conn) -> int:
    """Find recurring underspecification patterns across sessions."""
    rows = conn.execute("""
        SELECT underspecified_parts FROM session_judgments
        WHERE underspecified_parts IS NOT NULL AND underspecified_parts != '[]'
    """).fetchall()

    if len(rows) < 5:
        return 0

    # Count aspect frequencies
    aspect_counts = {}
    for (parts_json,) in rows:
        try:
            parts = (
                json.loads(parts_json) if isinstance(parts_json, str) else parts_json
            )
            for p in parts:
                aspect = p.get("aspect", str(p)) if isinstance(p, dict) else str(p)
                # Normalize
                aspect_lower = aspect.lower().strip()
                aspect_counts[aspect_lower] = aspect_counts.get(aspect_lower, 0) + 1
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    if not aspect_counts:
        return 0

    # Find aspects appearing in >20% of judged sessions
    threshold = len(rows) * 0.2
    recurring = [(a, c) for a, c in aspect_counts.items() if c >= threshold and c >= 3]
    recurring.sort(key=lambda x: -x[1])

    if not recurring:
        return 0

    top = recurring[:3]
    aspects_str = ", ".join(f"'{a}' ({c}x)" for a, c in top)

    conn.execute(
        """
        INSERT INTO prescriptions (category, title, description, evidence, confidence)
        VALUES (?, ?, ?, ?, ?)
    """,
        [
            "prompt_quality",
            "AI analysis: recurring underspecification patterns",
            f"LLM analysis found these aspects are frequently underspecified in your prompts: "
            f"{aspects_str}. Including these details upfront could reduce back-and-forth.",
            f"Based on analysis of {len(rows)} sessions with underspecified elements.",
            0.75,
        ],
    )
    return 1


# ---------------------------------------------------------------------------
# Judgment-based action generators (return dicts, no DB writes)
# ---------------------------------------------------------------------------


def _action_judgment_prompt_quality(conn):
    result = conn.execute("""
        SELECT AVG(prompt_clarity), AVG(prompt_completeness), COUNT(*)
        FROM session_judgments
    """).fetchone()
    if not result or not result[2] or result[2] < 5:
        return []

    avg_clarity, avg_completeness, n = result
    if avg_clarity < 0.6:
        return [
            {
                "type": "tip",
                "title": f"AI analysis: prompt clarity at {avg_clarity:.0%}",
                "body": f"Across {n} sessions, your prompts average {avg_clarity:.0%} clarity "
                f"and {avg_completeness:.0%} completeness. Be more specific upfront.",
                "evidence": f"AI analysis of {n} sessions",
            }
        ]
    return []


def _action_judgment_misalignment(conn):
    result = conn.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN misalignment_count > 0 THEN 1 ELSE 0 END) as misaligned
        FROM session_judgments
    """).fetchone()
    if not result or not result[0] or result[0] < 5:
        return []

    total, misaligned = result
    rate = misaligned / total
    if rate > 0.3:
        return [
            {
                "type": "warning",
                "title": f"AI analysis: {rate:.0%} of sessions have misalignments",
                "body": f"{misaligned} of {total} analyzed sessions had points where Claude "
                f"went off-track. Review the AI Analysis in session details for specifics.",
                "evidence": f"AI analysis of {total} sessions",
            }
        ]
    return []


def _action_judgment_underspec_patterns(conn):
    rows = conn.execute("""
        SELECT underspecified_parts FROM session_judgments
        WHERE underspecified_parts IS NOT NULL AND underspecified_parts != '[]'
    """).fetchall()

    if len(rows) < 5:
        return []

    aspect_counts = {}
    for (parts_json,) in rows:
        try:
            parts = (
                json.loads(parts_json) if isinstance(parts_json, str) else parts_json
            )
            for p in parts:
                aspect = p.get("aspect", str(p)) if isinstance(p, dict) else str(p)
                aspect_lower = aspect.lower().strip()
                aspect_counts[aspect_lower] = aspect_counts.get(aspect_lower, 0) + 1
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    threshold = len(rows) * 0.2
    recurring = [(a, c) for a, c in aspect_counts.items() if c >= threshold and c >= 3]
    recurring.sort(key=lambda x: -x[1])

    if not recurring:
        return []

    top = recurring[:3]
    return [
        {
            "type": "tip",
            "title": "AI analysis: recurring gaps in prompts",
            "body": f"Frequently underspecified: {', '.join(a for a, _ in top)}. "
            f"Including these details upfront could save turns.",
            "evidence": f"Pattern found in {len(rows)} sessions",
        }
    ]


# ---------------------------------------------------------------------------
# Behavioral correlation action generators (connect behaviors to AI outcomes)
# ---------------------------------------------------------------------------


def _action_prompt_length_correlation(conn):
    """Bin sessions by first-prompt length, compare avg productivity per bin."""
    rows = conn.execute("""
        SELECT
            CASE
                WHEN LENGTH(s.first_prompt) < 100 THEN 'under 100 chars'
                WHEN LENGTH(s.first_prompt) < 500 THEN '100-500 chars'
                ELSE 'over 500 chars'
            END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
        ORDER BY avg_prod DESC
    """).fetchall()

    if len(rows) < 2:
        return []

    best = rows[0]
    worst = rows[-1]
    delta = best[1] - worst[1]
    if abs(delta) < 0.05:
        return []

    return [
        {
            "type": "tip",
            "title": f"Prompt length matters: {best[0]} prompts are most productive",
            "body": f"Sessions with prompts {best[0]} had {best[1]:.0%} productivity vs "
            f"{worst[1]:.0%} for {worst[0]}.",
            "evidence": f"Based on {sum(r[2] for r in rows)} AI-judged sessions",
        }
    ]


def _action_correction_impact(conn):
    """Compare completion rates for zero-correction vs has-correction sessions."""
    rows = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero' ELSE 'has' END as bucket,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_pct,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bucket
        HAVING n >= 3
    """).fetchall()

    buckets = {r[0]: (r[1], r[2], r[3]) for r in rows}
    if "zero" not in buckets or "has" not in buckets:
        return []

    zero_comp, zero_prod, zero_n = buckets["zero"]
    has_comp, has_prod, has_n = buckets["has"]
    if abs(zero_comp - has_comp) < 5:
        return []

    return [
        {
            "type": "tip",
            "title": "Corrections correlate with lower completion",
            "body": f"Sessions with zero corrections completed {zero_comp:.0f}% of the time "
            f"vs {has_comp:.0f}% with corrections. Productivity: {zero_prod:.0%} vs {has_prod:.0%}.",
            "evidence": f"{zero_n} zero-correction vs {has_n} with-correction sessions",
        }
    ]


def _action_tool_focus(conn):
    """Compare productivity for focused (<5 unique tools) vs broad (5+) sessions."""
    rows = conn.execute("""
        SELECT
            CASE WHEN f.unique_tools_used < 5 THEN 'focused' ELSE 'broad' END as bucket,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bucket
        HAVING n >= 3
    """).fetchall()

    buckets = {r[0]: (r[1], r[2]) for r in rows}
    if "focused" not in buckets or "broad" not in buckets:
        return []

    f_prod, f_n = buckets["focused"]
    b_prod, b_n = buckets["broad"]
    delta = f_prod - b_prod
    if abs(delta) < 0.05:
        return []

    better = "focused (<5 tools)" if delta > 0 else "broad (5+ tools)"
    worse = "broad (5+ tools)" if delta > 0 else "focused (<5 tools)"
    hi = max(f_prod, b_prod)
    lo = min(f_prod, b_prod)

    return [
        {
            "type": "tip",
            "title": f"Tool focus: {better} sessions are more productive",
            "body": f"Sessions using {better} had {hi:.0%} productivity vs {lo:.0%} for {worse}.",
            "evidence": f"{f_n} focused vs {b_n} broad sessions",
        }
    ]


# ---------------------------------------------------------------------------
# Skill-gap prescription generators
# ---------------------------------------------------------------------------


def _skill_gap_prescriptions(conn) -> int:
    """Generate prescriptions from top skill gaps."""
    from .config import SKILL_DIMENSIONS, SKILL_NUDGES

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return 0

    cols = [d[0] for d in cursor.description]
    p = dict(zip(cols, profile))

    count = 0
    for gap_key in ["gap_1", "gap_2", "gap_3"]:
        dim_id = p.get(gap_key)
        if not dim_id:
            continue

        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        dim_name = dim_info.get("name", dim_id)
        dim_num = int(dim_id[1:])
        current_score = p.get(f"d{dim_num}_score", 0)
        current_level = int(current_score)
        target_level = current_level + 1

        nudge_text = SKILL_NUDGES.get((dim_id, target_level))
        if not nudge_text:
            continue

        conn.execute(
            """
            INSERT INTO prescriptions (category, title, description, evidence, confidence)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                "skill_gap",
                f"Level up {dim_name} (L{current_level} -> L{target_level})",
                nudge_text,
                f"Skill assessment across {p.get('session_count', 0)} sessions.",
                0.7,
            ],
        )
        count += 1

    return count


def _action_skill_gaps(conn):
    """Generate action items from skill gaps."""
    from .config import SKILL_DIMENSIONS, SKILL_NUDGES

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return []

    cols = [d[0] for d in cursor.description]
    p = dict(zip(cols, profile))

    actions = []
    for gap_key in ["gap_1", "gap_2", "gap_3"]:
        dim_id = p.get(gap_key)
        if not dim_id:
            continue

        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        dim_name = dim_info.get("name", dim_id)
        dim_num = int(dim_id[1:])
        current_score = p.get(f"d{dim_num}_score", 0)
        current_level = int(current_score)
        target_level = current_level + 1

        nudge_text = SKILL_NUDGES.get((dim_id, target_level))
        if not nudge_text:
            continue

        actions.append(
            {
                "type": "tip",
                "title": f"Level up {dim_name} (L{current_level} -> L{target_level})",
                "body": nudge_text,
                "evidence": f"Skill assessment across {p.get('session_count', 0)} sessions",
            }
        )

    return actions
