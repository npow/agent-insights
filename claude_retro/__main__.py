"""CLI entry point: serve, ingest, digest, reset, setup."""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

from .port_select import choose_server_port

LAUNCH_AGENT_LABEL = "com.claude-retro"
LAUNCH_AGENT_PATH = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_AGENT_LABEL}.plist"


def _parse_serve_flags(args: list[str]) -> tuple[int | None, bool]:
    """Parse `serve` flags.

    Supported:
    - --port N
    - --no-open
    """
    port_override = None
    no_open = False
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--no-open":
            no_open = True
            i += 1
            continue
        if a == "--port":
            if i + 1 >= len(args):
                raise ValueError("--port requires a value")
            try:
                p = int(args[i + 1])
            except ValueError as e:
                raise ValueError("--port must be an integer") from e
            if p < 1 or p > 65535:
                raise ValueError("--port must be between 1 and 65535")
            port_override = p
            i += 2
            continue
        raise ValueError(f"Unknown serve flag: {a}")
    return port_override, no_open


def _retro_program_args(port: int) -> tuple[list[str], str | None]:
    project_root = Path(__file__).resolve().parents[1]
    uv_bin = shutil.which("uv")
    if not uv_bin:
        for candidate in (
            Path.home() / ".local" / "bin" / "uv",
            Path("/opt/homebrew/bin/uv"),
            Path("/usr/local/bin/uv"),
        ):
            if candidate.exists():
                uv_bin = str(candidate)
                break
    # Source checkout: use uv run with explicit project for reproducible env.
    if uv_bin and (project_root / "pyproject.toml").exists():
        return (
            [
                uv_bin,
                "run",
                "--project",
                str(project_root),
                "python",
                "-m",
                "claude_retro",
                "serve",
                "--no-open",
                "--port",
                str(port),
            ],
            str(project_root),
        )
    # Installed package fallback.
    return (
        [
            sys.executable,
            "-m",
            "claude_retro",
            "serve",
            "--no-open",
            "--port",
            str(port),
        ],
        None,
    )


def _write_launch_agent(port: int, relay_port: int):
    LAUNCH_AGENT_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_dir = Path.home() / "Library" / "Logs"
    program_args, working_dir = _retro_program_args(port)
    plist_data = {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": program_args,
        "RunAtLoad": True,
        "KeepAlive": True,
        "EnvironmentVariables": {
            "CLAUDE_RETRO_PORT": str(port),
            "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{relay_port}",
        },
        "StandardOutPath": str(log_dir / "claude-retro.log"),
        "StandardErrorPath": str(log_dir / "claude-retro.err.log"),
    }
    if working_dir:
        plist_data["WorkingDirectory"] = working_dir
    with LAUNCH_AGENT_PATH.open("wb") as f:
        plistlib.dump(plist_data, f)


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def _setup_services():
    if sys.platform != "darwin":
        raise RuntimeError("setup is currently supported on macOS only")
    if shutil.which("launchctl") is None:
        raise RuntimeError("launchctl not found")
    if shutil.which("uv") is None:
        raise RuntimeError("uv not found. Install uv first: https://docs.astral.sh/uv/")

    relay_port = int(os.environ.get("CLAUDE_RETRO_RELAY_PORT", "18082"))
    if relay_port < 1 or relay_port > 65535:
        raise RuntimeError("CLAUDE_RETRO_RELAY_PORT must be between 1 and 65535")

    # Ensure relay tool exists and install/restart its launchd service.
    _run(["uv", "tool", "install", "--upgrade", "claude-relay"])
    _run(
        [
            "claude-relay",
            "service",
            "install",
            "--host",
            "127.0.0.1",
            "--port",
            str(relay_port),
        ]
    )
    _run(["claude-relay", "service", "restart"])

    # Resolve a stable, non-colliding port for Claude Retro.
    env_port = os.environ.get("CLAUDE_RETRO_PORT", "").strip()
    preferred = int(env_port) if env_port.isdigit() else None
    port, preferred_port = choose_server_port(preferred)
    _write_launch_agent(port, relay_port)

    # Reload launchd job.
    subprocess.run(
        ["launchctl", "unload", str(LAUNCH_AGENT_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )
    _run(["launchctl", "load", str(LAUNCH_AGENT_PATH)])
    _run(["launchctl", "start", LAUNCH_AGENT_LABEL])

    print("Setup complete.")
    print(f"  Claude Retro launch agent: {LAUNCH_AGENT_PATH}")
    if preferred_port != port:
        print(f"  Requested port {preferred_port} was busy; using {port}")
    else:
        print(f"  Claude Retro port: {port}")
    print(f"  Relay port: {relay_port}")
    print(f"  URL: http://localhost:{port}")


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
        from .server import app, set_worker
        from .background import IngestionWorker

        try:
            port_override, no_open = _parse_serve_flags(args[1:])
        except ValueError as e:
            print(f"Error: {e}")
            print("Usage: python -m claude_retro serve [--port N] [--no-open]")
            sys.exit(1)

        server_port, preferred = choose_server_port(port_override)

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

        url = f"http://localhost:{server_port}"
        if preferred != server_port:
            print(f"Port {preferred} is busy; using {server_port} instead.")
        print(f"Starting server on {url}")
        if not no_open:
            webbrowser.open(url)
        app.run(host="127.0.0.1", port=server_port, debug=False, threaded=False)

    elif command == "setup":
        try:
            _setup_services()
        except subprocess.CalledProcessError as e:
            msg = (e.stderr or e.stdout or "").strip()
            if msg:
                print(msg)
            print(f"Setup failed while running: {' '.join(e.cmd)}")
            sys.exit(1)
        except Exception as e:
            print(f"Setup failed: {e}")
            sys.exit(1)

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
        print("Usage: python -m claude_retro [serve|ingest|digest|reset|setup]")
        sys.exit(1)


if __name__ == "__main__":
    main()
