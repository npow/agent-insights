# Agent Insights

[![CI](https://github.com/npow/agent-insights/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/agent-insights/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A sprint retro for your AI coding sessions. Reads your local Claude Code history, scores every session, and tells you what to change.

## How is this different from Claude Code's built-in `/insights`?

Claude Code's `/insights` is a weekly LLM-generated report: it reads your sessions, identifies friction patterns, surfaces impressive moments, and suggests CLAUDE.md additions and new workflows. It's genuinely useful.

Agent Insights is a **persistent local server** you leave running alongside Claude Code. The key differences:

| | Built-in `/insights` | Agent Insights |
|---|---|---|
| Delivery | One-time HTML report | Live web UI, auto-refreshes every 30s |
| Cadence | Generated on demand (weekly snapshot) | Continuous — captures every session as it happens |
| Token & cost tracking | ✓ | ✓ |
| Response time distribution | ✓ | ✓ |
| Multi-clauding detection | ✓ | ✓ |
| Time-of-day analysis | ✓ | ✓ |
| Friction & pattern analysis | ✓ (narrative) | ✓ (quantitative + narrative) |
| Outcome tracking | ✓ | ✓ |
| Prescriptions ("change X") | ✓ | ✓ |
| CLAUDE.md suggestions | ✓ (copy-paste) | ✓ (generated per session, auto-applied) |
| Lines / files changed | ✓ | — |
| Per-session quality scores | — | ✓ convergence, drift, thrash |
| Session-level browsing | — | ✓ filterable feed, click to expand AI analysis |
| Historical trends | — | ✓ score charts over time |
| Skill radar | — | ✓ 9-dimension skill profile |

`/insights` is a great weekly snapshot. Agent Insights is a persistent dashboard that captures every session as it happens and goes further: per-session scores, a browsable session feed, trend charts, and CLAUDE.md rules that are written directly to your projects automatically.

## Install

```bash
pip install agent-insights
agent-insights
```

If `pip install agent-insights` returns `No matching distribution found`, install the currently published PyPI package name instead:

```bash
pip install claude-retro
claude-retro
```

For background startup on reboot (macOS), run:

```bash
agent-insights setup
```

If you installed `claude-retro` from PyPI, use `claude-retro setup` instead.

## What it looks like

### The Verdict
See your overall performance at a glance — completion rate, productivity bar, top issues.

![Verdict](https://github.com/npow/agent-insights/blob/main/screenshots/verdict.png?raw=true)

### Change These 3 Things
Concrete, actionable advice based on your actual session data.

![Change Cards](https://github.com/npow/agent-insights/blob/main/screenshots/changes.png?raw=true)

### Session Feed
Every session with outcome, productivity, and inline misalignment callouts. Click to expand full AI analysis with conversation context.

![Sessions](https://github.com/npow/agent-insights/blob/main/screenshots/sessions.png?raw=true)

### Charts & Project Health
Outcome distribution, score trends, baselines, activity heatmap, and per-project health table.

![Charts](https://github.com/npow/agent-insights/blob/main/screenshots/charts.png?raw=true)

## What you get

- **Verdict** — plain-English summary of your sessions with a productivity bar
- **Prescriptions** — top 3 things to change, sourced from AI analysis of your sessions
- **Session scores** — convergence, drift, thrash for every session
- **Prompt quality** — clarity and completeness grades, with specific gaps flagged
- **Waste analysis** — productive vs. wasted turns, misalignments with conversation context
- **Pattern detection** — recurring prompt gaps, misalignment themes, behavioral correlations
- **LLM Judge** — configurable parallelism (4-24), real-time progress bar

All analysis runs locally against `~/.claude/projects/`. Nothing leaves your machine except the LLM judging calls (which use your own `claude` CLI).

Multi-agent note: when `sessionlog` ingests Codex/Cursor/Antigravity sources, Agent Insights now preserves `agent_type` on sessions and exposes it in APIs (`/api/sessions`, `/api/projects`, `/api/live`, `/api/agent-types`).

## Quick start

```bash
pip install agent-insights
agent-insights
```

PyPI fallback (current published package name):

```bash
pip install claude-retro
claude-retro
```

Opens in your browser at `localhost:8420` (or the next free port if 8420 is busy). Sessions auto-refresh every 30 seconds. Hit "Run LLM Judge" to get AI analysis.

## Commands

| Command | Description |
|---------|-------------|
| `agent-insights` | Start server + open browser (default) |
| `agent-insights setup` | Install/start launchd services for Agent Insights + claude-relay (macOS) |
| `agent-insights ingest` | Run full pipeline including LLM judging |
| `agent-insights digest` | Print a weekly summary to stdout |
| `agent-insights reset` | Delete the database and start fresh |

If you installed from `pip install claude-retro`, use the same subcommands with the `claude-retro` executable.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_INSIGHTS_DB` | `~/.claude/agent-insights.sqlite` | Database path |
| `AGENT_INSIGHTS_PORT` | `8420` | Preferred server port (falls back if busy) |
| `AGENT_INSIGHTS_RELAY_PORT` | `18082` | Port for the auto-started `claude-relay` |
| `ANTHROPIC_BASE_URL` | *(auto)* | Override LLM endpoint (e.g. `https://api.anthropic.com`) |
| `ANTHROPIC_API_KEY` | `unused` | API key (only needed when using the real Anthropic API) |
| `SENTRY_DSN` | *(unset)* | Enable Sentry error reporting when set |
| `SENTRY_ENVIRONMENT` | `local` | Environment tag sent to Sentry (e.g. `dev`, `prod`) |
| `SENTRY_RELEASE` | package version | Optional release override sent to Sentry |
| `SENTRY_TRACES_SAMPLE_RATE` | `0.1` | Tracing sample rate between `0.0` and `1.0` |
| `SENTRY_PROFILES_SAMPLE_RATE` | *(unset)* | Optional profiling sample rate between `0.0` and `1.0` |

## Requirements

- Python 3.11+
- `claude` CLI on PATH (for LLM judging via the bundled `claude-relay`)

## Architecture

```
~/.claude/projects/**/*.jsonl          ~/.claude/agent-insights.sqlite
         |                                      ^
         v                                      |
 +-----------------+    every 30s    +----------+----------+
 | IngestionWorker |--------------->| ingest -> sessions  |
 |  (daemon thread)|                | -> features         |
 +-----------------+                | -> scores           |
                                    | -> intents          |
                                    | -> baselines        |
                                    | -> prescriptions    |
                                    +---------------------+
                                              ^
                                              |  on "Run LLM Judge"
                                    +---------------------+
                                    |    LLM Judge        |
                                    | (claude -p, 4-24x)  |
                                    +---------------------+

 +-------------------+
 |   Flask server    |<--- REST API ---> Browser
 |  (auto port)      |
 +-------------------+
```
