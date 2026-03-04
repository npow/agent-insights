"""Tests for skill tree assessment."""

from agent_insights.sessions import build_sessions, build_tool_usage
from agent_insights.features import extract_features
from agent_insights.skills import (
    assess_skills,
    _detect_context_mgmt,
    _detect_planning,
    _detect_prompt_craft,
    _detect_tool_leverage,
    _detect_verification,
    _detect_git_workflow,
    _detect_error_recovery,
    _detect_session_strategy,
)


def _make_data(**overrides):
    """Build a minimal data dict for detector testing."""
    base = {
        "session_id": "test-sess",
        "duration": 600,
        "user_prompt_count": 3,
        "tool_use_count": 5,
        "tool_error_count": 0,
        "turn_count": 6,
        "first_prompt": "Implement the auth module",
        "trajectory": "converged",
        "features": {
            "topic_keyword_entropy": 0.2,
            "correction_rate": 0.0,
            "correction_count": 0,
            "edit_write_ratio": 0.3,
            "bash_ratio": 0.2,
            "task_ratio": 0.0,
            "unique_tools_used": 4,
            "has_pr_link": False,
            "branch_switch_count": 0,
        },
        "user_texts": ["Implement the auth module", "Looks good", "Ship it"],
        "tool_names": ["Edit", "Write", "Read", "Grep"],
        "tool_usage": {
            "Edit": {"use_count": 3, "error_count": 0},
            "Write": {"use_count": 1, "error_count": 0},
            "Read": {"use_count": 5, "error_count": 0},
        },
        "tool_sequence": ["Read", "Read", "Edit", "Write", "Edit"],
        "files_modified": 4,
        "judgment": {
            "prompt_clarity": None,
            "prompt_completeness": None,
            "correction_count": None,
            "productivity_ratio": None,
            "outcome": None,
        },
    }
    base.update(overrides)
    return base


class TestContextMgmt:
    def test_baseline(self):
        data = _make_data()
        level, opp = _detect_context_mgmt(data)
        assert level == 1

    def test_compact_usage(self):
        data = _make_data(user_texts=["Fix bug", "/compact", "Now refactor"])
        level, opp = _detect_context_mgmt(data)
        assert level == 2

    def test_compact_with_focus(self):
        data = _make_data(
            user_texts=["Fix bug", "/compact focus on the auth module", "Refactor"]
        )
        level, opp = _detect_context_mgmt(data)
        assert level == 3

    def test_opportunity_high_entropy(self):
        data = _make_data(
            features={"topic_keyword_entropy": 0.7},
        )
        level, opp = _detect_context_mgmt(data)
        assert opp == 2

    def test_opportunity_long_session(self):
        data = _make_data(duration=2000)
        level, opp = _detect_context_mgmt(data)
        assert opp == 2


class TestPlanning:
    def test_baseline(self):
        level, opp = _detect_planning(_make_data())
        assert level == 1

    def test_numbered_steps(self):
        data = _make_data(user_texts=["Step 1: create the schema\nStep 2: add the API"])
        level, opp = _detect_planning(data)
        assert level == 2

    def test_plan_mode(self):
        data = _make_data(user_texts=["Use plan mode for this", "Implement it"])
        level, opp = _detect_planning(data)
        assert level == 3

    def test_opportunity_many_files_no_plan(self):
        data = _make_data(files_modified=6)
        level, opp = _detect_planning(data)
        assert opp == 3


class TestPromptCraft:
    def test_baseline_short(self):
        data = _make_data(first_prompt="Fix bug")
        level, opp = _detect_prompt_craft(data)
        assert level == 1

    def test_longer_prompt(self):
        data = _make_data(first_prompt="x" * 250)
        level, opp = _detect_prompt_craft(data)
        assert level == 2

    def test_file_ref(self):
        data = _make_data(user_texts=["Look at @file src/auth.py and fix the bug"])
        level, opp = _detect_prompt_craft(data)
        assert level == 3

    def test_acceptance_criteria(self):
        data = _make_data(
            user_texts=["Implement this but don't change the API", "should pass tests"]
        )
        level, opp = _detect_prompt_craft(data)
        assert level == 3

    def test_opportunity_high_correction(self):
        data = _make_data(features={"correction_rate": 0.4, "correction_count": 3})
        level, opp = _detect_prompt_craft(data)
        assert opp == 3


class TestToolLeverage:
    def test_baseline_few_tools(self):
        data = _make_data(
            features={"unique_tools_used": 2, "bash_ratio": 0.1, "task_ratio": 0.0}
        )
        level, opp = _detect_tool_leverage(data)
        assert level == 1

    def test_broad_tools(self):
        data = _make_data(
            features={"unique_tools_used": 5, "bash_ratio": 0.1, "task_ratio": 0.0}
        )
        level, opp = _detect_tool_leverage(data)
        assert level == 2

    def test_with_task_tool(self):
        data = _make_data(
            tool_names=["Edit", "Read", "Grep", "Glob", "Bash", "Task"],
            features={"unique_tools_used": 6, "bash_ratio": 0.1, "task_ratio": 0.1},
        )
        level, opp = _detect_tool_leverage(data)
        assert level == 3

    def test_opportunity_high_bash(self):
        data = _make_data(
            features={"unique_tools_used": 2, "bash_ratio": 0.7, "task_ratio": 0.0}
        )
        level, opp = _detect_tool_leverage(data)
        assert opp == 2


class TestVerification:
    def test_baseline(self):
        data = _make_data()
        level, opp = _detect_verification(data)
        assert level >= 1

    def test_test_mention(self):
        data = _make_data(user_texts=["Implement this", "Run pytest", "Ship it"])
        level, opp = _detect_verification(data)
        assert level == 2

    def test_opportunity_edits_no_tests(self):
        data = _make_data(
            features={"edit_write_ratio": 0.4},
            user_texts=["Implement auth"],
        )
        level, opp = _detect_verification(data)
        assert opp == 2


class TestGitWorkflow:
    def test_baseline(self):
        level, opp = _detect_git_workflow(_make_data())
        assert level == 1

    def test_commit_command(self):
        data = _make_data(user_texts=["/commit", "Done"])
        level, opp = _detect_git_workflow(data)
        assert level == 2

    def test_gh_pr(self):
        data = _make_data(user_texts=["gh pr create", "Done"])
        level, opp = _detect_git_workflow(data)
        assert level == 3


class TestErrorRecovery:
    def test_baseline(self):
        level, opp = _detect_error_recovery(_make_data())
        assert level == 1

    def test_with_error_context(self):
        data = _make_data(
            user_texts=["Here's the traceback: Error at line 42", "Fix it"]
        )
        level, opp = _detect_error_recovery(data)
        assert level == 2

    def test_root_cause(self):
        data = _make_data(
            user_texts=["Explain why this happens, don't fix yet", "Ok now fix it"]
        )
        level, opp = _detect_error_recovery(data)
        assert level == 3

    def test_opportunity_many_corrections(self):
        data = _make_data(features={"correction_count": 4})
        level, opp = _detect_error_recovery(data)
        assert opp == 3


class TestSessionStrategy:
    def test_focused_session(self):
        data = _make_data(duration=900, turn_count=10)
        level, opp = _detect_session_strategy(data)
        assert level == 2

    def test_resume_usage(self):
        data = _make_data(user_texts=["--continue from yesterday", "Now fix the tests"])
        level, opp = _detect_session_strategy(data)
        assert level == 3

    def test_opportunity_long_unfocused(self):
        data = _make_data(
            duration=4000,
            features={"topic_keyword_entropy": 0.6},
        )
        level, opp = _detect_session_strategy(data)
        assert opp == 2


class TestEdgeCases:
    """Adversarial edge cases that expose false positives and logic errors."""

    def test_planning_version_number_not_detected(self):
        """'Version 1.2' should NOT trigger numbered-steps planning."""
        data = _make_data(user_texts=["Fix the bug in version 1.2", "Ship it"])
        level, opp = _detect_planning(data)
        assert level == 1, (
            f"Version number '1.' should not trigger planning L2, got L{level}"
        )

    def test_planning_line_number_not_detected(self):
        """'line 1.' should NOT trigger numbered-steps planning."""
        data = _make_data(user_texts=["Error on line 1. Fix it", "Done"])
        level, opp = _detect_planning(data)
        assert level == 1, f"'line 1.' should not trigger planning L2, got L{level}"

    def test_background_color_not_session_strategy(self):
        """'background color' should NOT trigger session strategy L4."""
        data = _make_data(
            user_texts=["Change the background color to blue", "Looks good"],
            duration=600,
            turn_count=4,
        )
        level, opp = _detect_session_strategy(data)
        assert level < 4, f"'background color' should not trigger L4, got L{level}"

    def test_verification_mkdir_before_edit_not_test_first(self):
        """Bash(mkdir) before Edit should NOT count as test-first."""
        data = _make_data(
            tool_sequence=["Bash", "Read", "Edit", "Write"],
            user_texts=["Create the directory structure", "Now implement auth"],
        )
        level, opp = _detect_verification(data)
        assert level < 3, (
            f"Non-test Bash before Edit should not trigger L3, got L{level}"
        )

    def test_tool_leverage_standard_tools_not_mcp(self):
        """Standard Claude tools like EnterPlanMode should NOT count as MCP."""
        data = _make_data(
            tool_names=[
                "Edit",
                "Read",
                "Bash",
                "EnterPlanMode",
                "ExitPlanMode",
                "Skill",
            ],
            features={"unique_tools_used": 6, "bash_ratio": 0.1, "task_ratio": 0.0},
        )
        level, opp = _detect_tool_leverage(data)
        assert level < 4, f"Standard tools should not trigger MCP L4, got L{level}"

    def test_empty_session_no_crash(self):
        """Session with no user texts should not crash any detector."""
        data = _make_data(
            user_texts=[],
            first_prompt="",
            duration=0,
            turn_count=0,
            files_modified=0,
            tool_names=[],
            tool_usage={},
            tool_sequence=[],
        )
        # All detectors should return without error
        _detect_context_mgmt(data)
        _detect_planning(data)
        _detect_prompt_craft(data)
        _detect_tool_leverage(data)
        _detect_verification(data)
        _detect_git_workflow(data)
        _detect_error_recovery(data)
        _detect_session_strategy(data)

    def test_single_prompt_session(self):
        """Single-prompt abandoned session should produce valid assessments."""
        data = _make_data(
            user_texts=["Hello"],
            first_prompt="Hello",
            duration=5,
            turn_count=1,
            user_prompt_count=1,
        )
        level, opp = _detect_context_mgmt(data)
        assert level >= 1
        assert opp >= 0

    def test_planning_actual_step_pattern(self):
        """Actual planning with 'Step 1:' should still be detected."""
        data = _make_data(
            user_texts=["Step 1: create the database schema", "Step 2: write the API"]
        )
        level, opp = _detect_planning(data)
        assert level == 2

    def test_planning_actual_numbered_list(self):
        """Actual numbered list '1. foo\n2. bar' should still be detected."""
        data = _make_data(
            user_texts=["Please do:\n1. Create schema\n2. Add API\n3. Write tests"]
        )
        level, opp = _detect_planning(data)
        assert level == 2

    def test_background_agent_detected(self):
        """'background agent' should detect session strategy L4."""
        data = _make_data(
            user_texts=["Run this as a background agent for CI", "Check results"],
            duration=600,
            turn_count=4,
        )
        level, opp = _detect_session_strategy(data)
        assert level >= 4

    def test_run_in_background_detected(self):
        """'run in background' should detect session strategy L4."""
        data = _make_data(
            user_texts=["Run this task run_in_background=true", "Done"],
            duration=600,
            turn_count=4,
        )
        level, opp = _detect_session_strategy(data)
        assert level >= 4


class TestEndToEnd:
    def test_assess_skills_runs(self, seed_entries):
        """assess_skills runs without error on seeded data."""
        build_sessions()
        build_tool_usage()
        extract_features()
        n = assess_skills()
        assert n == 2  # sess-a and sess-b

        conn = seed_entries
        rows = conn.execute("SELECT COUNT(*) FROM session_skills").fetchone()[0]
        assert rows == 2

    def test_skill_profile_computed(self, seed_entries):
        """Skill profile is computed after assessment."""
        build_sessions()
        build_tool_usage()
        extract_features()
        assess_skills()

        conn = seed_entries
        profile = conn.execute("SELECT * FROM skill_profile WHERE id = 1").fetchone()
        assert profile is not None

    def test_idempotent(self, seed_entries):
        """Running assess_skills twice produces same count."""
        build_sessions()
        build_tool_usage()
        extract_features()
        n1 = assess_skills()
        n2 = assess_skills()
        assert n1 == n2

    def test_session_skills_have_values(self, seed_entries):
        """Session skills have non-negative level values."""
        build_sessions()
        build_tool_usage()
        extract_features()
        assess_skills()

        conn = seed_entries
        row = conn.execute(
            "SELECT d1_level, d3_level, d5_level FROM session_skills WHERE session_id = 'sess-a'"
        ).fetchone()
        assert row is not None
        for val in row:
            assert val >= 0
