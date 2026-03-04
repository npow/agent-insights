"""JSONL parsing and ingestion — delegated to sessionlog.ingest.

sessionlog.ingest provides:
- Correct agent_progress / bash_progress parsing (parentUuid on outer record)
- progress_entries table population
- conn.commit() fix (writes were previously lost)
- WAL-safe readers via autocommit connections

All public functions re-exported unchanged; callers in agent-insights continue
to work without modification.
"""

from sessionlog.ingest import (
    find_jsonl_files,
    needs_ingestion,
    mark_skip,
    clear_skip,
    parse_entry,
    parse_progress_entry,
    ingest_file,
    run_ingest,
)

__all__ = [
    "find_jsonl_files",
    "needs_ingestion",
    "mark_skip",
    "clear_skip",
    "parse_entry",
    "parse_progress_entry",
    "ingest_file",
    "run_ingest",
]
