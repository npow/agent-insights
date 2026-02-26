"""Tests for LLM judge — turn counting and validation logic."""

from claude_retro.sessions import build_sessions
from claude_retro.llm_judge import build_session_summary, _build_record


class TestBuildSessionSummary:
    def test_counts_user_and_assistant_turns(self, seed_entries):
        """Turn count includes both user prompts and assistant tool-call batches."""
        build_sessions()
        conn = seed_entries

        summary, turn_count = build_session_summary("sess-a", conn)

        # sess-a has: user prompt (turn 1), assistant tools (turn 2), user prompt (turn 3)
        assert turn_count == 3
        assert "TURN 1" in summary
        assert "TURN 2" in summary
        assert "TURN 3" in summary

    def test_tool_results_dont_count_as_turns(self, seed_entries):
        """Tool results (is_tool_result=True) should not get their own turn number."""
        build_sessions()
        conn = seed_entries

        summary, turn_count = build_session_summary("sess-b", conn)

        # sess-b: user (1), assistant tools (2), tool result (not a turn), user (3)
        assert turn_count == 3
        # Tool error must appear with clean tool name (not garbled JSON brackets)
        assert "Bash (error)" in summary
        assert "TURN 4" not in summary  # tool result must not create an extra turn

    def test_empty_session_returns_zero(self, conn):
        """Non-existent session returns empty summary and 0 turns."""
        summary, turns = build_session_summary("nonexistent", conn)
        assert summary == ""
        assert turns == 0


class TestBuildRecord:
    def test_productivity_validation_fixes_undercounted_turns(self):
        """If LLM returns fewer turns than expected, _build_record adjusts."""
        # Simulate: LLM says 1 productive + 0 waste but we know there are 10 turns
        import unittest.mock as mock

        def mock_combined(*args, **kwargs):
            return {
                "outcome": "completed",
                "outcome_confidence": 0.8,
                "outcome_reasoning": "test",
                "prompt_clarity": 0.7,
                "prompt_completeness": 0.6,
                "prompt_missing": [],
                "prompt_summary": "test",
                "trajectory_summary": "test",
                "underspecified_parts": [],
                "misalignment_count": 5,
                "misalignments": [
                    {"turn": i, "description": f"mistake {i}"} for i in range(1, 6)
                ],
                "correction_count": 0,
                "corrections": [],
                "productive_turns": 1,
                "waste_turns": 0,
                "productivity_ratio": 1.0,
                "waste_breakdown": {},
                "friction_categories": {},
                "_raw": "",
            }

        with mock.patch("claude_retro.llm_judge.analyze_combined", mock_combined):
            record = _build_record("test-session", "fake summary", turn_count=10)

        # Should NOT be 100% productive with 5 misalignments
        assert record["productive_turns"] + record["waste_turns"] == 10
        assert record["waste_turns"] >= 5, (
            f"With 5 misalignments, waste should be >= 5, got {record['waste_turns']}"
        )
        assert record["productivity_ratio"] < 0.8

    def test_consistent_ratio(self):
        """productivity_ratio always equals productive / (productive + waste)."""
        import unittest.mock as mock

        def mock_combined(*args, **kwargs):
            return {
                "outcome": "completed",
                "outcome_confidence": 0.9,
                "outcome_reasoning": "",
                "prompt_clarity": 0.8,
                "prompt_completeness": 0.7,
                "prompt_missing": [],
                "prompt_summary": "",
                "trajectory_summary": "",
                "underspecified_parts": [],
                "misalignment_count": 0,
                "misalignments": [],
                "correction_count": 0,
                "corrections": [],
                "productive_turns": 7,
                "waste_turns": 3,
                "productivity_ratio": 0.5,  # intentionally wrong — _build_record must recompute
                "waste_breakdown": {},
                "friction_categories": {},
                "_raw": "",
            }

        with mock.patch("claude_retro.llm_judge.analyze_combined", mock_combined):
            record = _build_record("test-session", "fake summary", turn_count=10)

        expected = record["productive_turns"] / (
            record["productive_turns"] + record["waste_turns"]
        )
        assert abs(record["productivity_ratio"] - expected) < 0.01, (
            f"Ratio {record['productivity_ratio']} != computed {expected}"
        )
