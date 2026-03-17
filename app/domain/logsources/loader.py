import yaml

from app.config.logging import get_logger
from app.domain.sigma_matcher.field_mapping import FieldMapping

logger = get_logger(__name__)


class LogsourceConfig:
    def __init__(self, parser_config: dict | None = None, field_mapping_str: str | None = None):
        self.parser_config = parser_config
        try:
            self.field_mapping = self._get_field_mapping_from_str(field_mapping_str)
        except Exception as e:
            logger.error("invalid field mapping, falling back to empty mapping", error=str(e))
            self.field_mapping = FieldMapping({})

    def __repr__(self) -> str:
        return f"LogsourceConfig(parser_config={self.parser_config}, field_mapping={self.field_mapping})"

    @staticmethod
    def _get_field_mapping_from_str(field_mapping_str: str | None) -> FieldMapping:
        """Parse yaml string to FieldMapping.

        Args:
            field_mapping_str: YAML string representing the field mapping

        Example:

        field_mapping:
          ```yaml
          sigma_field: event_field
          sigma_field2:
            - event_field2_1
            - event_field2_2
          ```
        result:
        ```json
        {
          "sigma_field": [
            "event_field"
          ],
          "sigma_field2": [
            "event_field2_1",
            "event_field2_2"
          ]
        }
        ```
        Returns:
            FieldMapping object

        Raises:
            ValueError: if field mapping is not a dictionary or contains invalid items
        """
        if not field_mapping_str:
            return FieldMapping({})

        d = yaml.safe_load(field_mapping_str)
        if not isinstance(d, dict):
            raise ValueError("field mapping must be a dictionary")

        field_mapping = {}
        for k, v in d.items():
            if isinstance(v, str):
                if not v:
                    continue
                field_mapping[k] = [v]
            elif isinstance(v, list):
                if not v:
                    continue
                field_mapping[k] = []
                for item in v:
                    if not item:
                        continue
                    if not isinstance(item, str):
                        raise ValueError(f"invalid field mapping for key {k}")
                    field_mapping[k].append(item)
            elif v is None:
                continue
            else:
                raise ValueError(f"invalid field mapping for key {k}")

        return FieldMapping(field_mapping)


def convert_kafka_event_to_logsource_config(
    event: dict | None,
) -> LogsourceConfig:
    if not event:
        return LogsourceConfig()

    parser_config = event.get("parser", {}).get("config")
    mapping = event.get("mapping")
    return LogsourceConfig(parser_config, mapping)
