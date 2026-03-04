"""Tests for session building — the most bug-prone area."""

import pytest
from agent_insights.sessions import build_sessions, build_tool_usage


class TestBuildSessions:
    def test_basic_aggregation(self, seed_entries):
        """Sessions are built from raw_entries with correct counts."""
        seed_entries.execute("UPDATE raw_entries SET agent_type = 'claude' WHERE session_id = 'sess-a'")
        seed_entries.execute("UPDATE raw_entries SET agent_type = 'codex' WHERE session_id = 'sess-b'")
        seed_entries.commit()

        n = build_sessions()
        assert n == 2  # sess-a and sess-b; sess-c excluded (single entry)

        conn = seed_entries
        rows = conn.execute(
            "SELECT session_id, project_name, agent_type, user_prompt_count, first_prompt "
            "FROM sessions ORDER BY session_id"
        ).fetchall()

        assert rows[0][0] == "sess-a"
        assert rows[0][1] == "proj-x"
        assert rows[0][2] == "claude"
        assert rows[0][3] == 2  # two user prompts
        assert "auth flow" in rows[0][4]

        assert rows[1][0] == "sess-b"
        assert rows[1][2] == "codex"
        assert rows[1][3] == 2  # two real user prompts (tool result doesn't count)

    def test_single_entry_session_excluded(self, seed_entries):
        """Sessions with only 1 entry are excluded (HAVING COUNT >= 2)."""
        build_sessions()
        conn = seed_entries
        found = conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE session_id = 'sess-c'"
        ).fetchone()[0]
        assert found == 0

    def test_no_duplicates(self, seed_entries):
        """build_sessions never produces duplicate session_ids."""
        build_sessions()
        conn = seed_entries
        dupes = conn.execute(
            "SELECT session_id, COUNT(*) as c FROM sessions "
            "GROUP BY session_id HAVING c > 1"
        ).fetchall()
        assert len(dupes) == 0

    def test_idempotent(self, seed_entries):
        """Running build_sessions twice produces the same result."""
        n1 = build_sessions()
        n2 = build_sessions()
        assert n1 == n2

    def test_rebuild_preserves_count(self, seed_entries):
        """Rebuilding sessions produces the same count (no data loss)."""
        n1 = build_sessions()
        conn = seed_entries
        count1 = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        assert count1 == n1

        # Rebuild again
        build_sessions()
        count2 = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        assert count2 == count1, "Rebuild should not lose sessions"


class TestBuildToolUsage:
    @pytest.mark.xfail(
        reason="build_tool_usage() is currently stubbed — does not parse tool_names"
    )
    def test_tool_usage_aggregation(self, seed_entries):
        """Tool usage is correctly aggregated per session."""
        build_sessions()
        build_tool_usage()
        conn = seed_entries

        tools_a = conn.execute(
            "SELECT tool_name, use_count FROM session_tool_usage "
            "WHERE session_id = 'sess-a' ORDER BY tool_name"
        ).fetchall()
        tool_names_a = [t[0] for t in tools_a]
        assert "Edit" in tool_names_a
        assert "Write" in tool_names_a

    def test_idempotent(self, seed_entries):
        """Running build_tool_usage twice produces the same result."""
        build_sessions()
        build_tool_usage()
        conn = seed_entries
        n1 = conn.execute("SELECT COUNT(*) FROM session_tool_usage").fetchone()[0]
        build_tool_usage()
        n2 = conn.execute("SELECT COUNT(*) FROM session_tool_usage").fetchone()[0]
        assert n1 == n2
