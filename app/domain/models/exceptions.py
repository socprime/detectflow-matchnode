"""
Custom exceptions for the application
"""


class AppException(Exception):  # noqa: N818
    """Base exception for all application errors"""

    pass


class ConfigurationError(AppException):
    """Configuration validation or loading error"""

    pass


class KafkaError(AppException):
    """Kafka connection or processing error"""

    pass
