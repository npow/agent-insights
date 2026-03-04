"""Background ingestion worker with simple mtime-based polling."""

import os
import threading
import traceback



class IngestionWorker(threading.Thread):
    """Daemon thread that polls ~/.claude/projects/ for changed JSONL files.

    Uses simple mtime-based polling every 30 seconds to detect changes.
    When changes are detected, runs the fast pipeline (everything except
    LLM judging, which is expensive and user-triggered).

    The ``status`` attribute is a dict visible to other threads:
      {"state": "idle"/"ingesting"/"judging", "step": "...", "ready": True/False,
       "current": N, "total": N}
    """

    def __init__(self, interval: float = 30.0, run_immediately: bool = False):
        super().__init__(daemon=True, name="ingestion-worker")
        self.interval = interval  # Polling interval
        self._run_immediately = run_immediately
        self._stop_event = threading.Event()
        self._known_mtimes: dict[str, float] = {}
        self.status: dict = {
            "state": "idle",
            "step": "",
            "ready": True,
            "current": 0,
            "total": 0,
            "last_error": None,
            "last_judged": 0,
        }
        self._refresh_request: dict | None = None
        self._refresh_lock = threading.Lock()

    def stop(self):
        self._stop_event.set()

    def request_refresh(self, concurrency: int = 12):
        """Request a full refresh (ingest + judge) from the UI thread.

        Non-blocking — sets a flag that the worker picks up on its next loop.
        """
        with self._refresh_lock:
            self._refresh_request = {"concurrency": concurrency}

    def request_fill_narratives(self, concurrency: int = 12):
        """Request a fill-narratives pass: re-judge sessions missing narrative text."""
        with self._refresh_lock:
            self._refresh_request = {
                "concurrency": concurrency,
                "fill_narratives": True,
            }

    @property
    def is_busy(self) -> bool:
        return self.status.get("state") not in ("idle",)

    def run(self):
        if self._run_immediately:
            try:
                self._run_pipeline()
            except Exception:
                traceback.print_exc()
                self._set_error(traceback.format_exc())

        while not self._stop_event.is_set():
            try:
                # Check for user-triggered refresh request
                req = None
                with self._refresh_lock:
                    if self._refresh_request:
                        req = self._refresh_request
                        self._refresh_request = None

                if req:
                    if req.get("fill_narratives"):
                        self._run_fill_narratives(req.get("concurrency", 12))
                    else:
                        self._run_full_refresh(req.get("concurrency", 12))
                elif self._has_changes():
                    # Simple mtime-based polling
                    self._run_pipeline()
            except Exception:
                traceback.print_exc()
                self._set_error(traceback.format_exc())
            self._stop_event.wait(self.interval)

    def _has_changes(self) -> bool:
        """Scan JSONL files across all agent sources and return True if any are new or modified."""
        from sessionlog.config import get_source_specs

        changed = False
        current_files: dict[str, float] = {}

        for _agent, source_dir in get_source_specs():
            if not source_dir.exists():
                continue
            for root, _dirs, files in os.walk(source_dir):
                for fname in files:
                    if not fname.endswith(".jsonl"):
                        continue
                    fpath = os.path.join(root, fname)
                    try:
                        mtime = os.path.getmtime(fpath)
                    except OSError:
                        continue
                    current_files[fpath] = mtime
                    if fpath not in self._known_mtimes or self._known_mtimes[fpath] < mtime:
                        changed = True

        self._known_mtimes = current_files
        return changed

    def _set_status(
        self, step: str, current: int = 0, total: int = 0, state: str = "ingesting"
    ):
        self.status = {
            "state": state,
            "step": step,
            "ready": False,
            "current": current,
            "total": total,
            "last_error": None,
            "last_judged": self.status.get("last_judged", 0),
        }

    def _set_idle(self, judged: int = 0):
        self.status = {
            "state": "idle",
            "step": "",
            "ready": True,
            "current": 0,
            "total": 0,
            "last_error": self.status.get("last_error"),
            "last_judged": judged,
        }

    def _set_error(self, error: str):
        # Use the last non-empty line (usually the exception message) as the short message
        lines = [l for l in error.strip().splitlines() if l.strip()]
        msg = lines[-1].strip() if lines else error[:300]
        # Cap at 300 chars so it fits in the UI
        if len(msg) > 300:
            msg = msg[:297] + "..."
        self.status = {
            "state": "idle",
            "step": "",
            "ready": True,
            "current": 0,
            "total": 0,
            "last_error": msg,
            "last_judged": self.status.get("last_judged", 0),
        }

    def _run_pipeline(self):
        """Run the fast ingestion pipeline (no LLM judging)."""
        from .ingest import run_ingest
        from .sessions import build_sessions, build_tool_usage
        from .features import extract_features
        from .skills import assess_skills
        from .scoring import compute_scores
        from .intents import classify_all_intents
        from .baselines import compute_baselines
        from .prescriptions import generate_prescriptions

        n = 10
        self._set_status("Ingesting JSONL files", 1, n)
        run_ingest()
        self._set_status("Building sessions", 2, n)
        build_sessions()
        self._set_status("Analyzing tool usage", 3, n)
        build_tool_usage()
        self._set_status("Extracting features", 4, n)
        extract_features()
        self._set_status("Assessing skills", 5, n)
        assess_skills()
        self._set_status("Computing scores", 6, n)
        compute_scores()
        self._set_status("Classifying intents", 7, n)
        classify_all_intents()
        self._set_status("Computing baselines", 8, n)
        compute_baselines()
        self._set_status("Generating prescriptions", 9, n)
        generate_prescriptions()
        self._set_status("Building search index", 10, n)
        from .db import rebuild_fts_index

        rebuild_fts_index()
        self._set_idle()

    def _run_full_refresh(self, concurrency: int = 12):
        """Run the full pipeline including LLM judging with progress."""
        from .ingest import run_ingest
        from .sessions import build_sessions, build_tool_usage
        from .features import extract_features
        from .skills import assess_skills
        from .scoring import compute_scores
        from .intents import classify_all_intents
        from .baselines import compute_baselines
        from .prescriptions import generate_prescriptions
        from .llm_judge import judge_sessions

        # Phase 1: fast pipeline (9 steps)
        n = 9
        self._set_status("Ingesting JSONL files", 1, n)
        run_ingest()
        self._set_status("Building sessions", 2, n)
        build_sessions()
        self._set_status("Analyzing tool usage", 3, n)
        build_tool_usage()
        self._set_status("Extracting features", 4, n)
        extract_features()
        self._set_status("Assessing skills", 5, n)
        assess_skills()
        self._set_status("Computing scores", 6, n)
        compute_scores()
        self._set_status("Classifying intents", 7, n)
        classify_all_intents()
        self._set_status("Computing baselines", 8, n)
        compute_baselines()
        self._set_status("Generating prescriptions", 9, n)
        generate_prescriptions()

        # Phase 2: LLM judging (reports per-session progress)
        def on_judge_progress(done, total, ok, errors):
            self._set_status(
                f"Judging sessions ({ok} ok, {errors} errors)",
                current=done,
                total=total,
                state="judging",
            )

        self._set_status("Starting LLM judge", 0, 0, state="judging")
        judged = judge_sessions(concurrency=concurrency, progress_callback=on_judge_progress)

        # Phase 3: recompute baselines/prescriptions with new judgments
        self._set_status("Recomputing baselines", 1, 2)
        compute_baselines()
        self._set_status("Regenerating prescriptions", 2, 2)
        generate_prescriptions()

        self._set_idle(judged=judged)

    def _run_fill_narratives(self, concurrency: int = 12):
        """Re-judge only sessions that are missing narrative text."""
        from .llm_judge import judge_sessions

        def on_progress(done, total, ok, errors):
            self._set_status(
                f"Filling narratives ({ok} ok, {errors} errors)",
                current=done,
                total=total,
                state="judging",
            )

        self._set_status("Counting sessions without narratives", 0, 0, state="judging")
        judged = judge_sessions(
            fill_narratives=True,
            concurrency=concurrency,
            progress_callback=on_progress,
        )
        if judged > 0:
            from .baselines import compute_baselines
            from .prescriptions import generate_prescriptions

            self._set_status("Recomputing baselines", 1, 2)
            compute_baselines()
            self._set_status("Regenerating prescriptions", 2, 2)
            generate_prescriptions()
        self._set_idle()
