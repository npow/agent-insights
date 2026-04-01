"""Configuration: paths, thresholds, scoring weights, keyword lists."""

import os
from pathlib import Path

# Ensure sessionlog (which also reads AGENT_INSIGHTS_DB) uses our default DB path.
# Must run before any sessionlog import.
os.environ.setdefault(
    "AGENT_INSIGHTS_DB", str(Path.home() / ".claude" / "agent-insights.sqlite")
)


# LLM relay — read port from relay package default so they stay in sync
def _relay_default_port() -> int:
    try:
        from agent_relay.__main__ import DEFAULT_PORT  # type: ignore[import]

        return DEFAULT_PORT
    except Exception:
        try:
            from claude_relay.__main__ import DEFAULT_PORT  # type: ignore[import]

            return DEFAULT_PORT
        except Exception:
            return 18082  # fallback if relay package not installed


RELAY_PORT = int(os.environ.get("AGENT_INSIGHTS_RELAY_PORT", _relay_default_port()))

# Paths
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DB_PATH = Path(
    os.environ.get(
        "AGENT_INSIGHTS_DB", Path.home() / ".claude" / "agent-insights.sqlite"
    )
)

# Scoring weights
CONVERGENCE_WEIGHTS = {
    "prompt_length_decrease": 0.25,
    "decision_markers": 0.20,
    "low_correction_rate": 0.20,
    "low_tool_error_rate": 0.15,
    "has_pr": 0.10,
    "stable_response_length": 0.10,
}

DRIFT_WEIGHTS = {
    "keyword_entropy": 0.25,
    "increasing_prompt_length": 0.20,
    "branch_switches": 0.15,
    "sidechain_ratio": 0.15,
    "no_decisions": 0.15,
    "long_session": 0.10,
}

THRASH_WEIGHTS = {
    "correction_rate": 0.30,
    "tool_error_rate": 0.25,
    "rephrasing": 0.20,
    "oscillating_lengths": 0.15,
    "api_errors": 0.10,
}

# Trajectory thresholds
TRAJECTORY_THRESHOLDS = {
    "converged": {"convergence_min": 0.6, "drift_max": 0.3, "thrash_max": 0.3},
    "drifted": {"drift_min": 0.5, "convergence_max": 0.4},
    "thrashed": {"thrash_min": 0.5, "convergence_max": 0.4},
}

# Baseline windows
BASELINE_WINDOWS = [14, 60]

# Intent keywords (keyword -> intent, weight)
INTENT_KEYWORDS = {
    "debug": [
        "bug",
        "error",
        "fix",
        "broken",
        "crash",
        "fail",
        "issue",
        "wrong",
        "traceback",
        "exception",
        "debug",
        "stack trace",
    ],
    "implement": [
        "add",
        "create",
        "build",
        "implement",
        "new feature",
        "write",
        "make",
        "generate",
    ],
    "refactor": [
        "refactor",
        "clean up",
        "restructure",
        "reorganize",
        "rename",
        "extract",
        "simplify",
        "move",
    ],
    "research": [
        "how does",
        "what is",
        "explain",
        "understand",
        "look at",
        "find",
        "search",
        "where is",
        "show me",
    ],
    "brainstorm": [
        "idea",
        "think about",
        "consider",
        "brainstorm",
        "design",
        "plan",
        "approach",
        "strategy",
        "option",
    ],
    "review": [
        "review",
        "check",
        "audit",
        "look over",
        "examine",
        "inspect",
        "pr",
        "pull request",
    ],
    "prototype": [
        "prototype",
        "proof of concept",
        "poc",
        "experiment",
        "try",
        "test",
        "spike",
        "explore",
    ],
}

# Decision markers
DECISION_MARKERS = [
    "let's go with",
    "i'll use",
    "decided to",
    "going with",
    "chosen",
    "the approach",
    "final",
    "commit",
    "ship",
    "merge",
    "lgtm",
    "looks good",
    "that works",
    "perfect",
    "done",
    "complete",
]

# Correction indicators
CORRECTION_MARKERS = [
    "actually",
    "wait",
    "no,",
    "sorry",
    "wrong",
    "instead",
    "not that",
    "undo",
    "revert",
    "go back",
    "try again",
    "that's not",
    "that didn't",
    "doesn't work",
    "not working",
]

# Rephrasing indicators
REPHRASING_MARKERS = [
    "i mean",
    "what i meant",
    "to clarify",
    "in other words",
    "let me rephrase",
    "more specifically",
    "to be clear",
]

# Tool categories for analysis
TOOL_CATEGORIES = {
    "edit_write": ["Edit", "Write", "NotebookEdit"],
    "read_grep": ["Read", "Grep", "Glob"],
    "bash": ["Bash"],
    "task": ["Task"],
    "web": ["WebFetch", "WebSearch"],
}

# ===== SKILL TREE =====
# 10 dimensions x 5 levels of Claude Code proficiency

SKILL_DIMENSIONS = {
    "D1": {
        "name": "Context Management",
        "short": "Context",
        "weight": 2.0,
        "color": "#6366f1",
    },
    "D2": {
        "name": "Planning & Decomposition",
        "short": "Planning",
        "weight": 1.0,
        "color": "#818cf8",
    },
    "D3": {
        "name": "Prompt Craft",
        "short": "Prompts",
        "weight": 1.5,
        "color": "#22c55e",
    },
    "D4": {
        "name": "CLAUDE.md Configuration",
        "short": "Config",
        "weight": 0.5,
        "color": "#eab308",
    },
    "D5": {
        "name": "Tool Leverage",
        "short": "Tools",
        "weight": 1.0,
        "color": "#3b82f6",
    },
    "D6": {
        "name": "Verification & QA",
        "short": "Verify",
        "weight": 1.5,
        "color": "#ef4444",
    },
    "D7": {
        "name": "Git Workflow",
        "short": "Git",
        "weight": 1.0,
        "color": "#f97316",
    },
    "D8": {
        "name": "Error Recovery",
        "short": "Errors",
        "weight": 1.0,
        "color": "#a855f7",
    },
    "D9": {
        "name": "Session Strategy",
        "short": "Sessions",
        "weight": 1.0,
        "color": "#06b6d4",
    },
    "D10": {
        "name": "Codebase Design",
        "short": "Design",
        "weight": 0.5,
        "color": "#8b8fa3",
    },
}

# Detection keywords per dimension
SKILL_CONTEXT_KEYWORDS = ["/clear", "/compact", "/context"]
SKILL_COMPACT_FOCUS = ["focus on", "only keep", "retain context"]
SKILL_PLAN_MARKERS = [
    "plan mode",
    "enterplanmode",
    "step 1",
    "step 2",
    "step 3",
    "1.",
    "2.",
    "3.",
    "numbered steps",
    "implementation plan",
    "spec.md",
    "SPEC.md",
]
SKILL_PROMPT_REFS = ["@file", "@folder", "@url"]
SKILL_ACCEPTANCE_CRITERIA = [
    "should pass",
    "don't change",
    "do not change",
    "must pass",
    "acceptance criteria",
    "expected output",
    "expected behavior",
]
SKILL_THINKING_TRIGGERS = [
    "think hard",
    "think carefully",
    "ultrathink",
    "think step by step",
    "reason through",
]
SKILL_TEST_COMMANDS = [
    "pytest",
    "npm test",
    "yarn test",
    "make test",
    "cargo test",
    "go test",
    "jest",
    "vitest",
    "mocha",
    "rspec",
    "unittest",
    "test_",
]
SKILL_ROOT_CAUSE = [
    "explain why",
    "don't fix yet",
    "do not fix",
    "root cause",
    "why is this",
    "what caused",
    "diagnose",
    "investigate first",
]
SKILL_GIT_COMMANDS = [
    "/commit",
    "gh pr",
    "gh issue",
    "git worktree",
    "git stash",
]
SKILL_SESSION_RESUME = ["--continue", "--resume", "background"]
SKILL_INIT_COMMANDS = ["/init", "claude.md", "CLAUDE.md"]

# Nudges: keyed by (dimension_id, target_level) -> nudge text
# These are fallbacks — running the LLM judge replaces them with evidence-backed insights from your actual sessions.
SKILL_NUDGES = {
    (
        "D1",
        2,
    ): "Your sessions aren't using context management commands. Long sessions accumulate stale context that causes Claude to repeat mistakes or forget earlier decisions. Try: '/compact focus on the current bug — drop the earlier architecture discussion'.",
    (
        "D1",
        3,
    ): "You're using /compact but without focus instructions, so irrelevant context stays alive. When switching tasks mid-session, use '/clear' to fully reset, or '/compact only keep the changes made to auth.py and the current error'.",
    (
        "D1",
        4,
    ): "Front-load all file reads before making changes in long sessions. Use @filename references instead of pasting content inline. When a session exceeds ~50 turns, split into a new session with a fresh summary prompt.",
    (
        "D2",
        2,
    ): "You're diving into implementation without an explicit plan. For tasks touching 3+ files, list the changes upfront first. Try: 'Before coding, list every file we'll need to change and what changes each needs. Don't start implementing yet.'",
    (
        "D2",
        3,
    ): "Use Plan Mode for complex tasks — it forces Claude to show its reasoning before touching code. Say: 'Enter plan mode and propose an implementation strategy. Don't write any code yet.' This catches wrong approaches before they cost turns.",
    (
        "D2",
        4,
    ): "Use the Task tool to run independent subtasks in parallel. Spin one agent to research the API, another to set up the test scaffold, while you review the plan — this can cut multi-hour sessions to minutes.",
    (
        "D3",
        2,
    ): "Your initial prompts often lack the specifics Claude needs to avoid correction loops. Include: the exact file path, the full error message, and what 'done' looks like. 'Fix the bug' → 'The test in auth_test.py:47 fails with KeyError: user_id — fix the implementation, not the test'.",
    (
        "D3",
        3,
    ): "Add explicit constraints upfront to prevent Claude going in the wrong direction. Before starting: 'Do not change the public API. Existing tests must still pass. Only modify the implementation inside _process_batch().' This prevents the most common correction loops.",
    (
        "D3",
        4,
    ): "For hard problems, activate extended reasoning: 'Think step by step before proposing a solution. Consider edge cases around concurrent writes and what happens if the queue is empty.' Front-loading analysis reduces back-and-forth on missed cases.",
    (
        "D4",
        2,
    ): "No CLAUDE.md detected. This file encodes your conventions so Claude doesn't need to be told them every session. Run '/init' to generate a starter, then customize it with your real preferences — test commands, style rules, things Claude should never do.",
    (
        "D5",
        2,
    ): "High Bash usage for file operations suggests underuse of Claude's built-in tools. Use Read/Edit/Grep/Glob directly — they're faster, have better error messages, and don't require shell quoting. Reserve Bash for running tests, builds, and CLI tools.",
    (
        "D5",
        3,
    ): "Use the Task tool for parallel research. Instead of blocking on 'find all usages of this function', spawn a Task agent for the search while you continue with the main work: 'Use the Task tool to search the codebase for X in the background'.",
    (
        "D6",
        2,
    ): "Sessions show file edits without test runs. Always end implementation prompts with: 'After making changes, run the test suite and show me the output. If any tests fail, fix them before presenting the result as done.'",
    (
        "D6",
        3,
    ): "Adopt test-first ordering: ask Claude to run the failing test first to confirm the failure, then fix the implementation. 'First run pytest test_auth.py::test_login -v to show the failure, then fix the code until it passes.'",
    (
        "D6",
        4,
    ): "After fixing a bug, habitually add: 'Now add a regression test that would have caught this bug.' Use /commit to create atomic checkpoints after each working change so you can safely revert.",
    (
        "D7",
        2,
    ): "Sessions have file changes but no commits. Use /commit to let Claude write commit messages from the diff — it's faster and produces better messages. Commit frequently to create safe rollback points.",
    (
        "D7",
        3,
    ): "Let Claude create PRs: 'Create a PR for these changes with a description of what was fixed and why.' Claude reads the diff and writes context-aware PR descriptions better than most humans.",
    (
        "D8",
        2,
    ): "Debug prompts are missing the full error context. Always paste the complete stack trace — not just the last line. The specific file, line number, and exception type often contain the answer. 'Here's the full traceback: [paste]. What's the root cause?'",
    (
        "D8",
        3,
    ): "When Claude fixes symptoms instead of causes, say: 'Don't change any code yet — trace this error back to its root cause and explain why it's happening.' This prevents multi-turn cycles chasing the same underlying bug.",
    (
        "D8",
        4,
    ): "After fixing a recurring bug: 'Add a regression test that would have caught this. Commit the fix separately from the test so the history is clean.' This creates both a safety net and useful git history.",
    (
        "D9",
        2,
    ): "Long sessions with unrelated tasks cause Claude to lose focus and mix up context between subtasks. Start a new Claude session for each distinct goal — sessions under 20 turns with a single task have significantly higher productivity.",
    (
        "D9",
        3,
    ): "Use --continue to resume your last session, or --resume [session-id] for a specific conversation. For interrupted multi-step work, start with: 'Summarize what we completed last session and what's still left to do.'",
    (
        "D9",
        4,
    ): "Run independent tasks in parallel Claude sessions. While one session is testing/building, start another for the next feature. Use background agents for CI-style workflows: 'Run this full test suite in the background and notify me when done.'",
}
