"""Version information for Agent Insights."""

import subprocess
from datetime import datetime

__version__ = "1.2.9"


def get_version_info() -> dict:
    """Get version, commit, and build date."""
    try:
        commit = (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode()
            .strip()
        )
    except Exception:
        commit = "unknown"

    try:
        version = (
            subprocess.check_output(
                ["git", "describe", "--tags", "--always", "--dirty"]
            )
            .decode()
            .strip()
        )
    except Exception:
        version = __version__

    return {
        "version": version,
        "commit": commit,
        "build_date": datetime.utcnow().isoformat() + "Z",
    }
