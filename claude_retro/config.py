"""Configuration: paths, thresholds, scoring weights, keyword lists."""

import os
from pathlib import Path

# Paths
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DB_PATH = Path(
    os.environ.get("CLAUDE_RETRO_DB", Path.home() / ".claude" / "retro.sqlite")
)
SERVER_PORT = int(os.environ.get("CLAUDE_RETRO_PORT", "8420"))

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
SKILL_NUDGES = {
    ("D1", 2): "Try using /compact when your context gets large. Add focus instructions like '/compact focus on the auth module'.",
    ("D1", 3): "Use /clear between distinct subtasks to reset context. Watch for topic drift in long sessions.",
    ("D1", 4): "Structure large tasks to front-load file reads. Use @file references to bring specific context in.",
    ("D2", 2): "For multi-file changes, describe your plan before asking Claude to implement. List the files and changes needed.",
    ("D2", 3): "Use Plan Mode (ask Claude to plan first) for complex tasks. Reference spec files for shared understanding.",
    ("D2", 4): "Use the Task tool to parallelize independent subtasks. Break work into focused sub-agents.",
    ("D3", 2): "Include file paths and error messages in your prompts. Be specific about what 'working' means.",
    ("D3", 3): "Add acceptance criteria: 'should pass tests', 'don't change the API'. Use @file to reference context.",
    ("D3", 4): "Use thinking triggers ('think hard about edge cases') for complex problems. Include constraints explicitly.",
    ("D4", 2): "Create a CLAUDE.md with your project's coding conventions. Run /init to generate a starter.",
    ("D5", 2): "Claude has dedicated Read/Edit/Glob/Grep tools. High Bash usage for file ops suggests underuse of built-in tools.",
    ("D5", 3): "Use the Task tool for parallel research. Leverage subagents for independent code searches.",
    ("D6", 2): "Ask Claude to run tests after making changes. Add 'run the tests' to your workflow.",
    ("D6", 3): "Write tests first, then implement. Claude can run test suites and iterate until they pass.",
    ("D6", 4): "Set up pre-commit hooks that Claude respects. Use /commit for atomic, well-messaged commits.",
    ("D7", 2): "Use /commit instead of manual git commands. Let Claude craft commit messages from the diff.",
    ("D7", 3): "Use 'gh pr create' through Claude for PR creation. Reference issues in commits.",
    ("D8", 2): "Paste full error messages with stack traces. Include reproduction steps.",
    ("D8", 3): "Before fixing, ask Claude to explain the root cause. Say 'explain why this happens, don't fix yet'.",
    ("D8", 4): "After fixing, ask Claude to add regression tests. Create checkpoints with git commits before risky changes.",
    ("D9", 2): "Keep sessions focused on one task. Start new sessions for new tasks instead of continuing long ones.",
    ("D9", 3): "Use --continue to resume interrupted sessions. Use --resume for picking up where you left off.",
    ("D9", 4): "Run multiple Claude sessions in parallel for independent tasks. Use background agents for CI-like workflows.",
}
