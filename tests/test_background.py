from agent_insights.background import IngestionWorker


def test_run_immediate_exception_reports_to_sentry(monkeypatch):
    worker = IngestionWorker(run_immediately=True)
    worker._stop_event.set()  # skip polling loop; only exercise immediate pass

    calls = []

    def _capture(exc):
        calls.append(str(exc))

    monkeypatch.setattr(
        "agent_insights.background.sentry_sdk.capture_exception", _capture
    )

    def _boom():
        raise RuntimeError("pipeline exploded")

    monkeypatch.setattr(worker, "_run_pipeline", _boom)

    worker.run()

    assert calls == ["pipeline exploded"]
    assert worker.status["last_error"] == "RuntimeError: pipeline exploded"
