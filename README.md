# Claude Retro

[![CI](https://github.com/npow/claude-retro/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/claude-retro/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A sprint retro for your AI coding sessions. Reads your local Claude Code history, scores every session, and tells you what to change.

## Install

```bash
pip install -e .
claude-retro
```

For background startup on reboot (macOS), run:

```bash
claude-retro setup
```

## What it looks like

### The Verdict
See your overall performance at a glance — completion rate, productivity bar, top issues.

![Verdict](https://github.com/npow/claude-retro/blob/main/screenshots/verdict.png?raw=true)

### Change These 3 Things
Concrete, actionable advice based on your actual session data.

![Change Cards](https://github.com/npow/claude-retro/blob/main/screenshots/changes.png?raw=true)

### Session Feed
Every session with outcome, productivity, and inline misalignment callouts. Click to expand full AI analysis with conversation context.

![Sessions](https://github.com/npow/claude-retro/blob/main/screenshots/sessions.png?raw=true)

### Charts & Project Health
Outcome distribution, score trends, baselines, activity heatmap, and per-project health table.

![Charts](https://github.com/npow/claude-retro/blob/main/screenshots/charts.png?raw=true)

## What you get

- **Verdict** — plain-English summary of your sessions with a productivity bar
- **Prescriptions** — top 3 things to change, sourced from AI analysis of your sessions
- **Session scores** — convergence, drift, thrash for every session
- **Prompt quality** — clarity and completeness grades, with specific gaps flagged
- **Waste analysis** — productive vs. wasted turns, misalignments with conversation context
- **Pattern detection** — recurring prompt gaps, misalignment themes, behavioral correlations
- **LLM Judge** — configurable parallelism (4-24), real-time progress bar

All analysis runs locally against `~/.claude/projects/`. Nothing leaves your machine except the LLM judging calls (which use your own `claude` CLI).

## Quick start

```bash
pip install -e .
claude-retro
```

Opens in your browser at `localhost:8420` (or the next free port if 8420 is busy). Sessions auto-refresh every 30 seconds. Hit "Run LLM Judge" to get AI analysis.

## Commands

| Command | Description |
|---------|-------------|
| `claude-retro` | Start server + open browser (default) |
| `claude-retro setup` | Install/start launchd services for Claude Retro + claude-relay (macOS) |
| `claude-retro ingest` | Run full pipeline including LLM judging |
| `claude-retro digest` | Print a weekly summary to stdout |
| `claude-retro reset` | Delete the database and start fresh |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAUDE_RETRO_DB` | `~/.claude/retro.duckdb` | Database path |
| `CLAUDE_RETRO_PORT` | `8420` | Preferred server port (falls back if busy) |
| `CLAUDE_RETRO_RELAY_PORT` | `18082` | Relay service port used by `claude-retro setup` |

## Requirements

- Python 3.10+
- `claude` CLI on PATH (for LLM judging)

## Architecture

```
~/.claude/projects/**/*.jsonl          ~/.claude/retro.duckdb
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
