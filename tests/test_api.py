"""Tests for API endpoints — catches the data consistency bugs we've hit."""

import pytest
from agent_insights.server import app
from agent_insights.sessions import build_sessions, build_tool_usage
from agent_insights.features import extract_features
from agent_insights.scoring import compute_scores
from agent_insights.intents import classify_all_intents


@pytest.fixture
def client(seed_entries):
    """Flask test client with seeded data."""
    build_sessions()
    build_tool_usage()
    extract_features()
    compute_scores()
    classify_all_intents()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestSessionsAPI:
    def test_returns_sessions(self, client):
        resp = client.get("/api/sessions?limit=10")
        data = resp.get_json()
        assert data["total"] == 2
        assert len(data["sessions"]) == 2
        assert all("agent_type" in s for s in data["sessions"])
        assert all(
            s["agent_type"] in ("unknown", "claude", "codex", "cursor", "antigravity")
            for s in data["sessions"]
        )

    def test_filters_by_agent_type(self, client, seed_entries):
        conn = seed_entries
        conn.execute(
            "UPDATE raw_entries SET agent_type = 'codex' WHERE session_id = 'sess-b'"
        )
        conn.execute(
            "UPDATE raw_entries SET agent_type = 'claude' WHERE session_id = 'sess-a'"
        )
        conn.commit()
        build_sessions()

        resp = client.get("/api/sessions?limit=10&agent_type=codex")
        data = resp.get_json()
        assert data["total"] == 1
        assert data["sessions"][0]["session_id"] == "sess-b"
        assert data["sessions"][0]["agent_type"] == "codex"

    def test_unjudged_sessions_have_null_productivity(self, client):
        """Unjudged sessions must return null, NOT 0, for productivity_ratio."""
        resp = client.get("/api/sessions?limit=10")
        data = resp.get_json()
        for s in data["sessions"]:
            assert s["productivity_ratio"] is None, (
                f"Unjudged session {s['session_id']} should have null productivity, got {s['productivity_ratio']}"
            )
            assert s["judgment_outcome"] is None

    def test_judged_session_has_productivity(self, client, seed_entries):
        """After adding a judgment, the session API returns it."""
        conn = seed_entries
        conn.execute("""
            INSERT INTO session_judgments (
                session_id, outcome, productivity_ratio,
                productive_turns, waste_turns, misalignment_count
            ) VALUES ('sess-a', 'completed', 0.8, 4, 1, 0)
        """)
        conn.commit()  # SQLite readers need explicit commit to see writer data

        resp = client.get("/api/sessions?limit=10&sort=started_at+ASC")
        data = resp.get_json()
        sess_a = next(s for s in data["sessions"] if s["session_id"] == "sess-a")
        assert sess_a["judgment_outcome"] == "completed"
        assert sess_a["productivity_ratio"] == 0.8


class TestJudgmentStatsAPI:
    def test_no_judgments(self, client):
        """When no judgments exist, total_judged is 0."""
        resp = client.get("/api/judgments/stats")
        data = resp.get_json()
        assert data["total_judged"] == 0

    def test_orphaned_judgments_excluded(self, client, seed_entries):
        """Judgments for non-existent sessions should not affect stats.

        This catches the bug where orphaned judgments inflated averages.
        """
        conn = seed_entries
        # Insert a judgment for a session that doesn't exist
        conn.execute("""
            INSERT INTO session_judgments (
                session_id, outcome, productivity_ratio,
                productive_turns, waste_turns, misalignment_count
            ) VALUES ('nonexistent-session', 'failed', 0.1, 1, 9, 5)
        """)

        # Also insert a real judgment
        conn.execute("""
            INSERT INTO session_judgments (
                session_id, outcome, productivity_ratio,
                productive_turns, waste_turns, misalignment_count
            ) VALUES ('sess-a', 'completed', 0.9, 9, 1, 0)
        """)
        conn.commit()  # SQLite readers need explicit commit to see writer data

        resp = client.get("/api/judgments/stats")
        data = resp.get_json()
        # The endpoint JOINs to sessions, so orphaned judgments (no matching session)
        # are excluded. Only sess-a judgment counts.
        assert data["total_judged"] == 1


class TestOverviewAPI:
    def test_returns_stats(self, client):
        resp = client.get("/api/overview")
        data = resp.get_json()
        assert data["total_sessions"] == 2
        assert data["total_projects"] >= 1
        assert "total_agent_types" in data


class TestProductivityConsistency:
    def test_ratio_matches_turns(self, seed_entries):
        """productivity_ratio must always equal productive_turns / (productive + waste)."""
        conn = seed_entries
        build_sessions()

        conn.execute("""
            INSERT INTO session_judgments (
                session_id, outcome, productive_turns, waste_turns,
                productivity_ratio, misalignment_count
            ) VALUES ('sess-a', 'completed', 7, 3, 0.7, 1)
        """)

        row = conn.execute("""
            SELECT productive_turns, waste_turns, productivity_ratio
            FROM session_judgments WHERE session_id = 'sess-a'
        """).fetchone()

        prod, waste, ratio = row
        expected = prod / (prod + waste) if (prod + waste) > 0 else 0
        assert abs(ratio - expected) < 0.01, (
            f"Stored ratio {ratio} doesn't match computed {expected} "
            f"(productive={prod}, waste={waste})"
        )
