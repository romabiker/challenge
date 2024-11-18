from core.celery import app
from events.use_cases.push_events_log import PushEventsLogRequest, PushEventsLogToClickhouse


@app.task(name="events.push_log_events_to_clickhouse")
def push_events_logs_to_clickhouse() -> None:
    PushEventsLogToClickhouse().execute(PushEventsLogRequest())
