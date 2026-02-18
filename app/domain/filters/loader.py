from app.config.logging import get_logger

logger = get_logger(__name__)


class Filter:
    def __init__(self, id: str, body: str):
        self.id = id
        self.body = body


def convert_filter_kafka_event_to_filter(event: dict) -> Filter | None:
    filter_id = event.get("filter_id")
    filter_body = event.get("filter_body")
    if not filter_id or not filter_body:
        logger.warning("Filter event missing filter_id or filter_body", event=event)
        return None

    return Filter(filter_id, filter_body)
