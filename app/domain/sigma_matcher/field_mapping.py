class FieldMapping:
    def __init__(self, field_mapping: dict[str, list[str]]):
        self.field_mapping = field_mapping

    def get_field_name_in_event(self, sigma_field_name: str) -> list[str]:
        """
        Returns the field names in the event that correspond
        to the original Windows Event Log field name that is used in Sigma rules.
        """
        mapped_fields = self.field_mapping.get(sigma_field_name)
        if mapped_fields:
            # Include original field name if not already in the mapped fields
            if sigma_field_name not in mapped_fields:
                return mapped_fields + [sigma_field_name]
            return mapped_fields
        return [sigma_field_name]
