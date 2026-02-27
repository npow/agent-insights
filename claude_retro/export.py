"""Export verdict and prescriptions to standalone HTML."""

import html
from datetime import datetime

from .db import get_conn


def generate_export_html() -> str:
    """Generate a standalone HTML report with verdict and top prescriptions."""
    conn = get_conn()

    # Get overview stats
    stats = conn.execute("""
        SELECT
            COUNT(*) as total_sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as total_hours,
            AVG(turn_count) as avg_turns
        FROM sessions
    """).fetchone()

    # Get prescriptions
    prescriptions = conn.execute("""
        SELECT title, description, evidence, confidence, category
        FROM prescriptions
        WHERE dismissed = FALSE
        ORDER BY confidence DESC
        LIMIT 3
    """).fetchall()

    # Get top sessions by judgment
    top_sessions = conn.execute("""
        SELECT
            s.session_id,
            s.project_name,
            s.started_at,
            s.first_prompt,
            j.outcome,
            j.outcome_confidence,
            j.prompt_summary
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        WHERE j.outcome IS NOT NULL
        ORDER BY j.outcome_confidence DESC
        LIMIT 5
    """).fetchall()

    # Build HTML
    total_sessions = stats[0] or 0
    avg_convergence = round(stats[1] or 0, 2)
    avg_drift = round(stats[2] or 0, 2)
    avg_thrash = round(stats[3] or 0, 2)
    total_hours = round(stats[4] or 0, 1)
    avg_turns = round(stats[5] or 0, 1)

    # Productivity calculation
    productivity = round(max(0, min(100, (avg_convergence * 100) - (avg_drift * 50) - (avg_thrash * 30))), 1)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Claude Retro Export - {datetime.now().strftime('%Y-%m-%d')}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #0f111a; color: #e2e4e9; line-height: 1.6; padding: 40px 20px;
}}
.container {{ max-width: 900px; margin: 0 auto; }}
h1 {{ font-size: 32px; margin-bottom: 30px; color: #fff; }}
h2 {{ font-size: 20px; margin: 30px 0 15px; color: #8b8fa3; }}
.verdict {{
  background: linear-gradient(135deg, #1a1d2e 0%, #16192a 100%);
  border: 1px solid #2a2d3a; border-radius: 12px; padding: 24px; margin-bottom: 30px;
}}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin: 20px 0; }}
.stat {{ text-align: center; }}
.stat-value {{ font-size: 28px; font-weight: 600; color: #6366f1; }}
.stat-label {{ font-size: 12px; color: #8b8fa3; text-transform: uppercase; letter-spacing: 0.5px; }}
.productivity-bar {{
  height: 24px; background: #1a1d2e; border-radius: 12px; overflow: hidden; margin: 20px 0;
}}
.productivity-fill {{
  height: 100%; background: linear-gradient(90deg, #10b981 0%, #6366f1 100%);
  display: flex; align-items: center; justify-content: flex-end; padding-right: 12px;
  font-size: 13px; font-weight: 600; color: white;
}}
.prescriptions {{ display: grid; gap: 16px; margin-bottom: 30px; }}
.card {{
  background: #1a1d2e; border: 1px solid #2a2d3a; border-radius: 8px; padding: 20px;
}}
.card-num {{ display: inline-block; width: 28px; height: 28px; background: #6366f1;
  border-radius: 50%; text-align: center; line-height: 28px; font-weight: 600; margin-right: 8px; }}
.card-title {{ font-size: 16px; font-weight: 600; margin-bottom: 8px; }}
.card-body {{ font-size: 14px; color: #a0a3b0; margin-bottom: 8px; }}
.card-evidence {{ font-size: 12px; color: #6366f1; }}
.sessions {{ display: grid; gap: 12px; }}
.session {{
  background: #1a1d2e; border: 1px solid #2a2d3a; border-radius: 8px; padding: 16px;
}}
.session-header {{ display: flex; justify-content: space-between; margin-bottom: 8px; }}
.session-project {{ font-weight: 600; color: #6366f1; }}
.session-date {{ font-size: 12px; color: #8b8fa3; }}
.session-outcome {{ display: inline-block; padding: 4px 8px; border-radius: 4px;
  font-size: 11px; font-weight: 600; background: rgba(99,102,241,0.1); color: #6366f1; }}
.session-prompt {{ font-size: 13px; color: #a0a3b0; margin-top: 8px; }}
.footer {{
  margin-top: 40px; padding-top: 20px; border-top: 1px solid #2a2d3a;
  text-align: center; font-size: 12px; color: #8b8fa3;
}}
</style>
</head>
<body>
<div class="container">
  <h1>Claude Retro Export</h1>
  <p style="color: #8b8fa3; margin-bottom: 30px;">Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>

  <div class="verdict">
    <h2>The Verdict</h2>
    <div class="stats">
      <div class="stat">
        <div class="stat-value">{total_sessions}</div>
        <div class="stat-label">Sessions</div>
      </div>
      <div class="stat">
        <div class="stat-value">{total_hours}h</div>
        <div class="stat-label">Total Time</div>
      </div>
      <div class="stat">
        <div class="stat-value">{avg_turns}</div>
        <div class="stat-label">Avg Turns</div>
      </div>
    </div>
    <div class="productivity-bar">
      <div class="productivity-fill" style="width: {productivity}%">{productivity}%</div>
    </div>
    <p style="text-align: center; color: #8b8fa3; font-size: 13px;">
      Productivity Score
    </p>
  </div>

  <h2>Change These 3 Things</h2>
  <div class="prescriptions">
"""

    # Add prescriptions
    for i, p in enumerate(prescriptions, 1):
        title = html.escape(p[0] or "")
        description = html.escape(p[1] or "")
        evidence = html.escape(p[2] or "")
        html_content += f"""
    <div class="card">
      <div class="card-num">{i}</div>
      <div class="card-title">{title}</div>
      <div class="card-body">{description}</div>
      <div class="card-evidence">{evidence}</div>
    </div>
"""

    html_content += """
  </div>

  <h2>Top Sessions</h2>
  <div class="sessions">
"""

    # Add sessions
    for s in top_sessions:
        session_id = s[0]
        project = html.escape(s[1] or "")
        from datetime import datetime as _dt
        try:
            date = _dt.fromisoformat(s[2]).strftime('%b %d, %Y') if s[2] else ""
        except (ValueError, TypeError):
            date = (s[2] or "")[:10]
        prompt = html.escape((s[3] or "")[:150] + "..." if s[3] and len(s[3]) > 150 else s[3] or "")
        outcome = html.escape(s[4] or "unknown")
        confidence = round((s[5] or 0) * 100, 0)

        html_content += f"""
    <div class="session">
      <div class="session-header">
        <div class="session-project">{project}</div>
        <div class="session-date">{date}</div>
      </div>
      <div><span class="session-outcome">{outcome} ({confidence}%)</span></div>
      <div class="session-prompt">{prompt}</div>
    </div>
"""

    html_content += """
  </div>

  <div class="footer">
    Generated by <strong>Claude Retro</strong> · Analyze your AI coding sessions
  </div>
</div>
</body>
</html>
"""

    return html_content


def export_to_file(file_path: str) -> None:
    """Export verdict to HTML file."""
    html = generate_export_html()
    with open(file_path, "w") as f:
        f.write(html)
