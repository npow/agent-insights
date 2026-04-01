"""CLI entry point: app, serve, ingest, digest, reset."""

import os
import sys

from .config import RELAY_PORT


def _ensure_relay(port: int = RELAY_PORT) -> bool:
    """Start agent-relay if it isn't already listening on the given port.

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
        print(f"  agent-relay already running on port {port}")
        return True

    # Don't start relay inside a Claude Code session (would fail with nested session error)
    if os.environ.get("CLAUDECODE"):
        print("  Skipping agent-relay auto-start (running inside Claude Code session).")
        print(
            f"  To enable LLM Judge, run in a separate terminal: agent-relay serve --port {port}"
        )
        return False

    relay_bin = shutil.which("agent-relay") or shutil.which("claude-relay")
    if not relay_bin:
        print("  Warning: agent-relay not found on PATH — LLM Judge will not work.")
        return False

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    print(f"  Starting agent-relay on port {port}...")
    try:
        proc = subprocess.Popen(
            [relay_bin, "serve", "--port", str(port)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach so it survives if this process dies
        )
    except Exception as e:
        print(f"  Warning: failed to start agent-relay: {e}")
        return False

    # Wait up to 8 seconds for the relay to become ready
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if _is_port_open(port):
            print(f"  agent-relay ready (pid {proc.pid})")
            return True
        if proc.poll() is not None:
            print(
                f"  Warning: agent-relay exited (code {proc.returncode}) — LLM Judge will not work."
            )
            return False
        time.sleep(0.25)

    print(
        "  Warning: agent-relay didn't become ready in time — LLM Judge may not work."
    )
    return False


_PLIST_LABEL = "com.agent-insights.server"
_PLIST_DIR = os.path.expanduser("~/Library/LaunchAgents")


def _setup_launchd():
    """Install a macOS launchd agent so agent-insights starts on login."""
    import shutil
    import subprocess

    if sys.platform != "darwin":
        print("Error: setup is only supported on macOS (launchd).")
        sys.exit(1)

    agent_insights_bin = shutil.which("agent-insights")
    if not agent_insights_bin:
        # Fall back to running via python -m
        agent_insights_bin = sys.executable
        program_args = f"""    <array>
        <string>{agent_insights_bin}</string>
        <string>-m</string>
        <string>agent_insights</string>
        <string>serve</string>
        <string>--no-open</string>
    </array>"""
    else:
        program_args = f"""    <array>
        <string>{agent_insights_bin}</string>
        <string>serve</string>
        <string>--no-open</string>
    </array>"""

    plist_path = os.path.join(_PLIST_DIR, f"{_PLIST_LABEL}.plist")
    log_dir = os.path.expanduser("~/.claude/logs")
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(_PLIST_DIR, exist_ok=True)

    # Capture current PATH so launchd can find agent-relay, claude, etc.
    current_path = os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
    env_vars = f"""    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{current_path}</string>"""
    # Pass through ANTHROPIC_API_KEY if set
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        env_vars += f"""
        <key>ANTHROPIC_API_KEY</key>
        <string>{api_key}</string>"""
    env_vars += """
    </dict>"""

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{_PLIST_LABEL}</string>
    <key>ProgramArguments</key>
{program_args}
{env_vars}
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log_dir}/agent-insights.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/agent-insights.err</string>
</dict>
</plist>
"""

    # Unload existing service if present
    try:
        subprocess.run(
            ["launchctl", "unload", plist_path],
            capture_output=True,
        )
    except Exception:
        pass

    with open(plist_path, "w") as f:
        f.write(plist_content)
    print(f"Wrote {plist_path}")

    subprocess.run(["launchctl", "load", plist_path], check=True)
    print(f"Loaded {_PLIST_LABEL} — agent-insights will start on login.")
    print(f"Logs: {log_dir}/agent-insights.log")
    print()
    print("To uninstall:")
    print(f"  launchctl unload {plist_path}")
    print(f"  rm {plist_path}")


def main():
    args = sys.argv[1:]
    command = args[0] if args else "serve"
    command_args = args[1:] if args else []
    from .telemetry import init_sentry

    init_sentry(component="cli", command=command, enable_flask=(command == "serve"))

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

        no_open = "--no-open" in command_args
        explicit_port = None
        if "--port" in command_args:
            idx = command_args.index("--port")
            if idx + 1 >= len(command_args):
                print(
                    "Usage: python -m agent_insights serve [--no-open] [--port <port>]"
                )
                sys.exit(1)
            try:
                explicit_port = int(command_args[idx + 1])
            except ValueError:
                print("Error: --port must be an integer")
                sys.exit(1)

        # Start agent-relay for LLM judging only when the user did not provide
        # an explicit ANTHROPIC_BASE_URL. If provided (including localhost),
        # respect it so users can target custom local providers (e.g. Ollama).
        _existing_url = os.environ.get("ANTHROPIC_BASE_URL", "")
        _user_supplied_base = bool(_existing_url)
        if not _user_supplied_base:
            relay_port = RELAY_PORT
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

        if explicit_port is not None:
            SERVER_PORT = explicit_port
        else:
            env_port = os.environ.get("AGENT_INSIGHTS_PORT", "").strip()
            if env_port:
                try:
                    SERVER_PORT = int(env_port)
                except ValueError:
                    print("Error: AGENT_INSIGHTS_PORT must be an integer")
                    sys.exit(1)
            else:
                SERVER_PORT, _ = choose_server_port()
        url = f"http://localhost:{SERVER_PORT}"
        print(f"Starting server on {url}")
        if not no_open:
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

    elif command == "setup":
        _setup_launchd()

    elif command == "help":
        print("Usage: agent-insights <command>")
        print()
        print("Commands:")
        print("  serve    Start server + open browser (default)")
        print("  ingest   Run full pipeline including LLM judging")
        print("  digest   Print a weekly summary to stdout")
        print("  reset    Delete the database and start fresh")
        print("  setup    Install macOS launchd service for auto-start on login")
        print("  help     Show this help message")

    else:
        print("Usage: agent-insights [serve|ingest|digest|reset|setup|help]")
        sys.exit(1)


if __name__ == "__main__":
    main()
