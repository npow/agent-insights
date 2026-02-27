"""Server port selection with collision fallback and persisted state."""

from __future__ import annotations

import os
import socket
from pathlib import Path

DEFAULT_SERVER_PORT = 8420
PORT_SCAN_LIMIT = 100
PORT_STATE_PATH = Path.home() / ".claude" / "retro-port"


def _is_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _read_saved_port() -> int | None:
    try:
        raw = PORT_STATE_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw.isdigit():
        return None
    port = int(raw)
    if port < 1 or port > 65535:
        return None
    return port


def _persist_port(port: int) -> None:
    try:
        PORT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        PORT_STATE_PATH.write_text(str(port), encoding="utf-8")
    except OSError:
        # Non-fatal: server can still run without persisted port.
        pass


def choose_server_port(preferred_port: int | None = None) -> tuple[int, int]:
    """Pick a free localhost port for the server.

    Returns (chosen_port, preferred_port).
    Preference order:
    1) CLAUDE_RETRO_PORT env var (if set)
    2) Previously persisted port
    3) DEFAULT_SERVER_PORT

    If preferred is taken, scans upward for a free port.
    """
    preferred: int | None = preferred_port
    if preferred is None:
        env_port = os.environ.get("CLAUDE_RETRO_PORT", "").strip()
        if env_port:
            try:
                candidate = int(env_port)
                if 1 <= candidate <= 65535:
                    preferred = candidate
            except ValueError:
                preferred = None
    if preferred is None:
        preferred = _read_saved_port() or DEFAULT_SERVER_PORT

    upper = min(65535, preferred + PORT_SCAN_LIMIT)
    for port in range(preferred, upper + 1):
        if _is_port_available(port):
            _persist_port(port)
            return port, preferred

    # Final fallback: ask OS for an ephemeral free port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])
    _persist_port(port)
    return port, preferred
