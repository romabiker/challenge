from collections.abc import Generator
from unittest.mock import ANY

import pytest
from clickhouse_connect.driver import Client
from django.conf import settings

from core.base_model import Model
from events.models import EventLog
from events.use_cases.create_events_log import CreateEventLog, CreateEventLogRequest
from events.use_cases.push_events_log import PushEventsLogRequest, PushEventsLogToClickhouse

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def push_events_logs_to_clickhouse_use_case() -> PushEventsLogToClickhouse:
    return PushEventsLogToClickhouse()


@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> Generator:
    f_ch_client.query(f'TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}')
    yield


def test_event_log_entry_created_and_pushed(
    f_ch_client: Client,
    push_events_logs_to_clickhouse_use_case: PushEventsLogToClickhouse,
) -> None:
    class TestEventCreated(Model):
        name: str

    event = TestEventCreated(name='Test')
    event_context = event.model_dump_json()

    event_log_request = CreateEventLogRequest(event=event)
    CreateEventLog().execute(event_log_request)

    event_log_qs = EventLog.objects.filter(
        event_type='test_event_created',
        environment='Local',
        event_context=event_context,
    )
    assert event_log_qs.filter(is_pushed_to_clickhouse=False).exists()
    push_events_log_request = PushEventsLogRequest()
    push_events_logs_to_clickhouse_use_case.execute(push_events_log_request)
    assert event_log_qs.filter(is_pushed_to_clickhouse=True).exists()

    log = f_ch_client.query("SELECT * FROM default.event_log WHERE event_type = 'test_event_created'")

    assert log.result_rows == [
        (
            'test_event_created',
            ANY,
            'Local',
            event_context,
            1,
        ),
    ]
