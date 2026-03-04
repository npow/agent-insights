"""SQLite database — thin wrapper around sessionlog.db.

All connection management, WAL mode, and base schema (raw_entries, sessions,
session_features, progress_entries, etc.) live in sessionlog.db.

This module re-exports those functions and applies the additional
agent-insights-specific schema extensions (extra columns on synthesis and
session_judgments) on top of the base schema.

DB_PATH is determined by the AGENT_INSIGHTS_DB env var (defaulted in config.py
before any sessionlog import can read it).
"""

import sqlite3

# config.py must be imported first — it sets AGENT_INSIGHTS_DB default so that
# sessionlog.config reads the correct path when it first imports.
from .config import DB_PATH  # noqa: F401 — re-exported for __main__.py reset command

from sessionlog.db import (
    get_writer as _at_get_writer,
    get_reader,
    get_conn,
    execute_write,
    execute_read,
    rebuild_fts_index,
)

__all__ = [
    "DB_PATH",
    "get_writer",
    "get_reader",
    "get_conn",
    "execute_write",
    "execute_read",
    "rebuild_fts_index",
]


def _migrate_add_columns(conn: sqlite3.Connection, table: str, columns: list):
    """Add columns to table if they don't exist (safe migration)."""
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")


def _init_extra_schema(conn: sqlite3.Connection):
    """Apply agent-insights-specific schema extensions on top of the sessionlog base."""
    # synthesis has extra columns in agent-insights not present in sessionlog
    _migrate_add_columns(
        conn,
        "synthesis",
        [
            ("workflow_prompts", "TEXT"),
            ("features_to_try", "TEXT"),
            ("session_count", "INTEGER DEFAULT 0"),
            ("productivity_avg", "REAL DEFAULT 0"),
            ("friction_counts", "TEXT"),  # JSON: {type: count}
            ("skill_levels", "TEXT"),  # JSON: {dim: level}
        ],
    )
    # synthesis_history — one row per judge run (previous values before overwrite)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS synthesis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            at_a_glance TEXT,
            usage_narrative TEXT,
            top_wins TEXT,
            top_friction TEXT,
            claude_md_additions TEXT,
            fun_headline TEXT,
            workflow_prompts TEXT,
            features_to_try TEXT,
            session_count INTEGER DEFAULT 0,
            productivity_avg REAL DEFAULT 0,
            friction_counts TEXT,
            skill_levels TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # session_judgments has extra columns in agent-insights
    _migrate_add_columns(
        conn,
        "session_judgments",
        [
            ("friction_categories", "TEXT"),
            ("estimated_cost_usd", "REAL"),
            ("handoff_memo", "TEXT"),
            ("rewrite_memo", "TEXT"),
        ],
    )
    # session_tool_usage gains timing columns
    _migrate_add_columns(
        conn,
        "session_tool_usage",
        [
            ("total_duration_ms", "INTEGER DEFAULT 0"),
            ("avg_duration_ms", "REAL DEFAULT 0"),
        ],
    )
    # Backfill newer sessionlog columns when running against older wheels.
    _migrate_add_columns(
        conn,
        "raw_entries",
        [
            ("agent_type", "TEXT DEFAULT 'unknown'"),
        ],
    )
    # sessions gains canonical agent type for multi-agent installs.
    _migrate_add_columns(
        conn,
        "sessions",
        [
            ("agent_type", "TEXT DEFAULT 'unknown'"),
        ],
    )
    conn.commit()


_extra_initialized = False


def get_writer() -> sqlite3.Connection:
    """Get the serialized writer connection with full schema.

    Calls sessionlog's get_writer (which creates the base schema), then
    applies agent-insights-specific schema extensions once per process.
    """
    global _extra_initialized
    conn = _at_get_writer()
    if not _extra_initialized:
        _init_extra_schema(conn)
        _extra_initialized = True
    return conn
