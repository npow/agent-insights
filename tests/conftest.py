"""Shared fixtures for tests — uses a temp SQLite DB for isolation."""

import json
import pytest

import agent_insights.config as _cr_cfg
from agent_insights import db

# sessionlog internals (db.py / config.py) are only present in the editable
# install from the git repo, not in the published PyPI wheel.  Guard so tests
# still run when only the PyPI package is installed.
try:
    import sessionlog.db as _at_db
    import sessionlog.config as _at_cfg

    _HAS_AGENTTRACE_INTERNALS = True
except ImportError:
    _at_db = None  # type: ignore[assignment]
    _at_cfg = None  # type: ignore[assignment]
    _HAS_AGENTTRACE_INTERNALS = False


@pytest.fixture(autouse=True)
def isolated_db(monkeypatch, tmp_path):
    """Redirect all DB access to a temp file for test isolation."""
    test_db = tmp_path / "test.sqlite"

    # Set env var so both sessionlog.config and agent_insights.config use the test DB.
    monkeypatch.setenv("AGENT_INSIGHTS_DB", str(test_db))

    # Patch DB_PATH everywhere it is referenced.
    monkeypatch.setattr(_cr_cfg, "DB_PATH", test_db)
    monkeypatch.setattr(db, "DB_PATH", test_db)

    if _HAS_AGENTTRACE_INTERNALS:
        # sessionlog.db._connect() looks up DB_PATH in its own namespace.
        monkeypatch.setattr(_at_db, "DB_PATH", test_db)
        monkeypatch.setattr(_at_cfg, "DB_PATH", test_db)

    # Reset the writer connection so the next get_writer() creates a fresh one
    # pointing at the test DB.
    old_writer = _at_db._writer_conn if _HAS_AGENTTRACE_INTERNALS else None
    if _HAS_AGENTTRACE_INTERNALS:
        _at_db._writer_conn = None

    # Reset agent_insights.db extra-schema flag so it re-runs on new connection.
    import agent_insights.db as _cr_db

    _cr_db._extra_initialized = False

    # Reset thread-local reader connection
    if _HAS_AGENTTRACE_INTERNALS and hasattr(_at_db._local, "reader"):
        try:
            _at_db._local.reader.close()
        except Exception:
            pass
        del _at_db._local.reader

    yield test_db

    # Teardown: restore original writer, close test connections
    if _HAS_AGENTTRACE_INTERNALS:
        try:
            test_conn = _at_db._writer_conn
            if test_conn is not None:
                test_conn.close()
        except Exception:
            pass
        _at_db._writer_conn = old_writer

    _cr_db._extra_initialized = False

    if _HAS_AGENTTRACE_INTERNALS and hasattr(_at_db._local, "reader"):
        try:
            _at_db._local.reader.close()
        except Exception:
            pass
        del _at_db._local.reader


@pytest.fixture
def conn(isolated_db):
    """Return a SQLite connection with full schema initialized."""
    return db.get_writer()


@pytest.fixture
def seed_entries(conn):
    """Insert minimal raw_entries for two sessions — one good, one single-entry."""
    from datetime import datetime, timedelta

    base = datetime(2026, 1, 15, 10, 0, 0)

    entries = [
        # Session A: 3 entries (user prompt, assistant tools, user prompt)
        (
            "e1",
            "sess-a",
            "proj-x",
            "user",
            base,
            None,
            False,
            "Implement auth flow",
            20,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e2",
            "sess-a",
            "proj-x",
            "assistant",
            base + timedelta(seconds=10),
            None,
            False,
            None,
            0,
            False,
            False,
            "claude-sonnet",
            json.dumps([]),
            json.dumps(["Edit", "Write"]),
            "done",
            100,
            500,
            200,
            None,
            0,
            None,
            None,
        ),
        (
            "e3",
            "sess-a",
            "proj-x",
            "user",
            base + timedelta(seconds=30),
            None,
            False,
            "Actually use JWT not sessions",
            30,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        # Session B: 4 entries (user, assistant with error, tool result error, user correction)
        (
            "e4",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5),
            None,
            False,
            "Fix the deploy bug",
            18,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e5",
            "sess-b",
            "proj-y",
            "assistant",
            base + timedelta(minutes=5, seconds=10),
            None,
            False,
            None,
            0,
            False,
            False,
            "claude-sonnet",
            json.dumps([]),
            json.dumps(["Bash", "Read"]),
            "running",
            50,
            300,
            100,
            None,
            0,
            None,
            None,
        ),
        (
            "e6",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5, seconds=15),
            None,
            False,
            None,
            0,
            True,
            True,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e7",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5, seconds=30),
            None,
            False,
            "Wrong file, try src/deploy.py",
            28,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        # Session A: system turn_duration entry (so turn_count >= 1 filter passes)
        (
            "e_sys_a",
            "sess-a",
            "proj-x",
            "system",
            base + timedelta(seconds=40),
            None,
            False,
            None,
            0,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            "turn_duration",
            5000,
            None,
            None,
        ),
        # Session B: system turn_duration entry
        (
            "e_sys_b",
            "sess-b",
            "proj-y",
            "system",
            base + timedelta(minutes=5, seconds=40),
            None,
            False,
            None,
            0,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            "turn_duration",
            3000,
            None,
            None,
        ),
        # Session C: single entry (should be excluded from sessions, HAVING COUNT >= 2)
        (
            "e8",
            "sess-c",
            "proj-x",
            "user",
            base + timedelta(minutes=10),
            None,
            False,
            "Hello",
            5,
            False,
            False,
            None,
            json.dumps([]),
            json.dumps([]),
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
    ]

    for e in entries:
        conn.execute(
            """
            INSERT INTO raw_entries (
                entry_id, session_id, project_name, entry_type, timestamp_utc,
                parent_uuid, is_sidechain, user_text, user_text_length,
                is_tool_result, tool_result_error, model, content_types,
                tool_names, text_content, text_length, input_tokens,
                output_tokens, system_subtype, duration_ms, git_branch, cwd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            list(e),
        )

    conn.commit()
    return conn
