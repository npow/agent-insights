"""CLI entry point: app, serve, ingest, digest, reset."""

import os
import sys


def _ensure_relay(port: int = 8082) -> bool:
    """Start claude-relay if it isn't already listening on the given port.

    Returns True if the relay is ready (was already running or we started it),
    False if we couldn't start it (degrade gracefully — LLM judging just won't work).
    """
    import shutil
    import socket
    import subprocess
    import time

    def _is_port_open(p: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", p), timeout=1):
                return True
        except OSError:
            return False

    if _is_port_open(port):
        print(f"  claude-relay already running on port {port}")
        return True

    # Don't start relay inside a Claude Code session (would fail with nested session error)
    if os.environ.get("CLAUDECODE"):
        print(f"  Skipping claude-relay auto-start (running inside Claude Code session).")
        print(f"  To enable LLM Judge, run in a separate terminal: claude-relay serve --port {port}")
        return False

    relay_bin = shutil.which("claude-relay")
    if not relay_bin:
        print("  Warning: claude-relay not found on PATH — LLM Judge will not work.")
        return False

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    print(f"  Starting claude-relay on port {port}...")
    try:
        proc = subprocess.Popen(
            [relay_bin, "serve", "--port", str(port)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach so it survives if this process dies
        )
    except Exception as e:
        print(f"  Warning: failed to start claude-relay: {e}")
        return False

    # Wait up to 8 seconds for the relay to become ready
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if _is_port_open(port):
            print(f"  claude-relay ready (pid {proc.pid})")
            return True
        if proc.poll() is not None:
            print(f"  Warning: claude-relay exited (code {proc.returncode}) — LLM Judge will not work.")
            return False
        time.sleep(0.25)

    print(f"  Warning: claude-relay didn't become ready in time — LLM Judge may not work.")
    return False


def main():
    args = sys.argv[1:]
    command = args[0] if args else "serve"

    if command == "ingest":
        from .ingest import run_ingest
        from .sessions import build_sessions, build_tool_usage
        from .features import extract_features
        from .skills import assess_skills
        from .scoring import compute_scores
        from .intents import classify_all_intents
        from .baselines import compute_baselines
        from .prescriptions import generate_prescriptions
        from .llm_judge import judge_sessions

        print("Ingesting JSONL files...")
        stats = run_ingest()
        print(
            f"  Files: {stats['total_files']} total, {stats['ingested_files']} ingested, {stats['skipped_files']} skipped"
        )
        print(
            f"  Entries: {stats['total_entries']} new, {stats['total_entries_in_db']} total in DB"
        )
        print(f"  Sessions found: {stats['total_sessions_found']}")
        print(f"  Projects: {stats['total_projects']}")

        print("Building sessions...")
        n = build_sessions()
        print(f"  {n} sessions built")

        print("Building tool usage...")
        n = build_tool_usage()
        print(f"  {n} tool usage records")

        print("Extracting features...")
        n = extract_features()
        print(f"  {n} sessions processed")

        print("Assessing skills...")
        n = assess_skills()
        print(f"  {n} sessions assessed")

        print("Computing scores...")
        n = compute_scores()
        print(f"  {n} sessions scored")

        print("Classifying intents...")
        n = classify_all_intents()
        print(f"  {n} sessions classified")

        print("Judging sessions (LLM analysis)...")
        n = judge_sessions()
        print(f"  {n} sessions judged")

        print("Computing baselines...")
        compute_baselines()
        print("  Done")

        print("Generating prescriptions...")
        n = generate_prescriptions()
        print(f"  {n} prescriptions generated")

        print("\nIngestion complete!")

    elif command == "serve":
        import webbrowser
        from .port_select import choose_server_port
        from .server import app, set_worker
        from .background import IngestionWorker

        # Start claude-relay for LLM judging.
        # Skip only if the user explicitly pointed us at a non-local LLM endpoint
        # (i.e. ANTHROPIC_BASE_URL is set AND is not localhost/127.0.0.1).
        _existing_url = os.environ.get("ANTHROPIC_BASE_URL", "")
        _user_supplied_remote = _existing_url and "localhost" not in _existing_url and "127.0.0.1" not in _existing_url
        if not _user_supplied_remote:
            relay_port = int(os.environ.get("CLAUDE_RETRO_RELAY_PORT", 8082))
            print("Checking LLM relay...")
            _ensure_relay(port=relay_port)
            # Always point the LLM judge at our local relay
            os.environ["ANTHROPIC_BASE_URL"] = f"http://localhost:{relay_port}"
            os.environ.setdefault("ANTHROPIC_API_KEY", "unused")

        # Check if DB is empty — worker will run pipeline immediately
        from .db import get_conn, get_writer

        # Ensure schema exists by calling get_writer() first
        get_writer()

        conn = get_conn()
        try:
            count = conn.execute("SELECT COUNT(*) FROM raw_entries").fetchone()[0]
            needs_ingest = count == 0
        except Exception:
            needs_ingest = True
        if needs_ingest:
            print("No data found. Ingesting in background...")

        # Start background worker
        worker = IngestionWorker(run_immediately=needs_ingest)
        set_worker(worker)
        worker.start()

        SERVER_PORT, _ = choose_server_port()
        url = f"http://localhost:{SERVER_PORT}"
        print(f"Starting server on {url}")
        webbrowser.open(url)
        app.run(host="127.0.0.1", port=SERVER_PORT, debug=False, threaded=False)

    elif command == "digest":
        from .digest import weekly_digest

        print(weekly_digest())

    elif command == "reset":
        from .config import DB_PATH

        if DB_PATH.exists():
            DB_PATH.unlink()
            print(f"Deleted {DB_PATH}")
        else:
            print("No database to reset.")

    else:
        print("Usage: python -m claude_retro [ingest|serve|digest|reset]")
        sys.exit(1)


if __name__ == "__main__":
    main()
