"""Weekly CLI summary — powered by AI judgment data."""

import json

from .db import get_conn


def weekly_digest() -> str:
    """Generate a formatted weekly summary using AI judgment data."""
    conn = get_conn()
    lines = []

    lines.append("=" * 60)
    lines.append("  CLAUDE CODE WEEKLY DIGEST")
    lines.append("=" * 60)

    # --- This week stats ---
    this_week = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(duration_seconds) / 3600.0 as hours
        FROM sessions
        WHERE started_at >= current_date - INTERVAL '7 days'
    """).fetchone()

    last_week = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(duration_seconds) / 3600.0 as hours
        FROM sessions
        WHERE started_at >= current_date - INTERVAL '14 days'
          AND started_at < current_date - INTERVAL '7 days'
    """).fetchone()

    # AI judgment stats for this week
    tw_judgment = conn.execute("""
        SELECT
            COUNT(*) as judged,
            AVG(j.productivity_ratio) as avg_prod,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN j.outcome = 'partially_completed' THEN 1 ELSE 0 END) as partial,
            SUM(CASE WHEN j.outcome = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN j.outcome = 'abandoned' THEN 1 ELSE 0 END) as abandoned,
            AVG(j.misalignment_count) as avg_misalign
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        WHERE s.started_at >= current_date - INTERVAL '7 days'
    """).fetchone()

    lw_judgment = conn.execute("""
        SELECT
            COUNT(*) as judged,
            AVG(j.productivity_ratio) as avg_prod,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) as completed,
            AVG(j.misalignment_count) as avg_misalign
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        WHERE s.started_at >= current_date - INTERVAL '14 days'
          AND s.started_at < current_date - INTERVAL '7 days'
    """).fetchone()

    use_alltime = this_week[0] == 0
    if use_alltime:
        lines.append("\n  No sessions in the last 7 days. Showing all-time stats.\n")
        this_week = conn.execute("""
            SELECT COUNT(*), SUM(duration_seconds) / 3600.0 FROM sessions
        """).fetchone()
        tw_judgment = conn.execute("""
            SELECT COUNT(*), AVG(j.productivity_ratio),
                   SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN j.outcome = 'partially_completed' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN j.outcome = 'failed' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN j.outcome = 'abandoned' THEN 1 ELSE 0 END),
                   AVG(j.misalignment_count)
            FROM session_judgments j
        """).fetchone()

    total_sess = this_week[0]
    total_hours = this_week[1] or 0
    judged = tw_judgment[0] if tw_judgment else 0
    avg_prod = tw_judgment[1] if tw_judgment else 0
    completed = int(tw_judgment[2] or 0) if tw_judgment else 0
    partial = int(tw_judgment[3] or 0) if tw_judgment else 0
    failed = int(tw_judgment[4] or 0) if tw_judgment else 0
    abandoned = int(tw_judgment[5] or 0) if tw_judgment else 0
    avg_misalign = tw_judgment[6] or 0 if tw_judgment else 0
    completion_rate = completed / judged if judged else 0

    period = "All-time" if use_alltime else "This Week"
    lines.append(f"\n  {period} Summary")
    lines.append(f"  {'-' * 40}")
    lines.append(f"  Sessions:        {total_sess}  ({total_hours:.1f}h)")
    lines.append(
        f"  Outcomes:        {completed} completed, {partial} partial, {failed} failed, {abandoned} abandoned"
    )
    lines.append(f"  Completion Rate: {completion_rate:.0%}")
    lines.append(
        f"  Avg Productivity:{avg_prod:.0%}" if avg_prod else "  Avg Productivity: N/A"
    )
    lines.append(f"  Avg Misalign:    {avg_misalign:.1f} per session")

    # --- Week-over-week comparison ---
    if not use_alltime and last_week[0] > 0 and lw_judgment[0] > 0:
        lw_prod = lw_judgment[1] or 0
        lw_completed = int(lw_judgment[2] or 0)
        lw_comp_rate = lw_completed / lw_judgment[0] if lw_judgment[0] else 0
        lw_misalign = lw_judgment[3] or 0

        lines.append(f"\n  vs Last Week ({last_week[0]} sessions)")
        lines.append(f"  {'-' * 40}")

        prod_delta = (avg_prod or 0) - lw_prod
        comp_delta = completion_rate - lw_comp_rate
        mis_delta = (avg_misalign or 0) - lw_misalign

        def arrow(d):
            return "+" if d > 0 else ""

        lines.append(
            f"  Productivity:    {arrow(prod_delta)}{prod_delta:.0%} ({lw_prod:.0%} -> {avg_prod:.0%})"
        )
        lines.append(
            f"  Completion Rate: {arrow(comp_delta)}{comp_delta:.0%} ({lw_comp_rate:.0%} -> {completion_rate:.0%})"
        )
        trend = "fewer" if mis_delta < 0 else "more"
        lines.append(
            f"  Misalignments:   {arrow(mis_delta)}{mis_delta:.1f}/session ({trend})"
        )

    # --- Top prompt gaps this week ---
    time_filter = (
        "WHERE s.started_at >= current_date - INTERVAL '7 days'"
        if not use_alltime
        else ""
    )
    join_clause = (
        "JOIN sessions s ON j.session_id = s.session_id" if not use_alltime else ""
    )
    gap_rows = conn.execute(
        f"""
        SELECT j.prompt_missing FROM session_judgments j
        {join_clause}
        {time_filter}
    """.replace("WHERE s.", "WHERE s.")
        if not use_alltime
        else """
        SELECT prompt_missing FROM session_judgments
        WHERE prompt_missing IS NOT NULL AND prompt_missing != '[]'
    """
    ).fetchall()

    gap_categories = {
        "context": [
            "repo",
            "codebase",
            "file",
            "directory",
            "structure",
            "existing",
            "path",
        ],
        "requirements": [
            "expected",
            "behavior",
            "output",
            "format",
            "specific",
            "requirement",
        ],
        "constraints": ["environment", "version", "dependency", "platform", "setup"],
        "error_details": ["error", "message", "stack", "trace", "log", "exception"],
        "scope": ["which", "where", "boundary", "limit", "priority"],
    }
    cat_counts = {c: 0 for c in gap_categories}
    for (raw,) in gap_rows:
        if not raw:
            continue
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                text = str(item).lower()
                for cat, kws in gap_categories.items():
                    if any(kw in text for kw in kws):
                        cat_counts[cat] += 1
                        break
        except (json.JSONDecodeError, TypeError):
            continue

    top_gaps = sorted(
        [(c, n) for c, n in cat_counts.items() if n > 0], key=lambda x: -x[1]
    )[:3]
    if top_gaps:
        lines.append("\n  Top Prompt Gaps")
        lines.append(f"  {'-' * 40}")
        for cat, count in top_gaps:
            lines.append(f"    {cat:20s} {count:3d} occurrences")

    # --- Worst session ---
    worst_query = """
        SELECT j.session_id, j.misalignment_count, j.outcome,
               LEFT(s.first_prompt, 80) as preview
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
    """
    if not use_alltime:
        worst_query += " WHERE s.started_at >= current_date - INTERVAL '7 days'"
    worst_query += " ORDER BY j.misalignment_count DESC LIMIT 1"
    worst = conn.execute(worst_query).fetchone()
    if worst and worst[1] > 0:
        lines.append("\n  Worst Session")
        lines.append(f"  {'-' * 40}")
        lines.append(f"    {worst[1]} misalignments | outcome: {worst[2]}")
        lines.append(f"    Prompt: {worst[3]}...")
        lines.append(f"    ID: {worst[0][:16]}...")

    # --- Active prescriptions ---
    prescriptions = conn.execute("""
        SELECT title, confidence FROM prescriptions
        WHERE dismissed = FALSE
        ORDER BY confidence DESC LIMIT 5
    """).fetchall()
    if prescriptions:
        lines.append("\n  Active Insights")
        lines.append(f"  {'-' * 40}")
        for title, conf in prescriptions:
            lines.append(f"    [{conf:.0%}] {title}")

    # --- Top projects ---
    lines.append("\n  Top Projects (by sessions)")
    lines.append(f"  {'-' * 40}")
    projects = conn.execute("""
        SELECT s.project_name, COUNT(*) as n,
               AVG(j.productivity_ratio) as avg_prod,
               SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(j.session_id), 0) as comp_rate
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY s.project_name
        ORDER BY n DESC LIMIT 5
    """).fetchall()
    for proj, n, prod, comp in projects:
        short = proj[:30]
        prod_str = f"prod={prod:.0%}" if prod else "prod=N/A"
        comp_str = f"comp={comp:.0%}" if comp else "comp=N/A"
        lines.append(f"    {short:30s} {n:3d} sessions  {prod_str}  {comp_str}")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)
