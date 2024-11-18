import structlog

from core.event_log_client import EVENT_LOG_COLUMNS, EventLogClient
from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from events.models import EventLog

logger = structlog.get_logger(__name__)


class PushEventsLogRequest(UseCaseRequest):
    pass


class PushEventsLogResponse(UseCaseResponse):
    result: int | None = None
    error: str = ''


class PushEventsLogToClickhouse(UseCase):

    def _execute(self, request: PushEventsLogRequest) -> PushEventsLogResponse:  # noqa: ARG002

        with EventLogClient.init() as client:
            event_logs_data = self._select_events_log_data_to_push()
            if not len(event_logs_data):
                return PushEventsLogResponse()

            event_logs_ids = []
            event_logs_to_push = []
            for items in event_logs_data:
                event_logs_ids.append(items[-1])
                event_logs_to_push.append(items[:-1])
                client.insert(data=event_logs_to_push)

            EventLog.objects.filter(id__in=event_logs_ids).update(is_pushed_to_clickhouse=True)
            logger.info(f'pushed {len(event_logs_to_push)} log events into clickhouse')

        return PushEventsLogResponse(result=len(event_logs_to_push))

    def _select_events_log_data_to_push(self) -> list[tuple]:
        return list(
            EventLog.objects
            .select_for_update()
            .filter(is_pushed_to_clickhouse=False)
            #  In Postgres, the true comes first, in SQLite, false comes first
            .order_by('-is_pushed_to_clickhouse', '-id')
            .values_list(*EVENT_LOG_COLUMNS, 'id'),
        )
