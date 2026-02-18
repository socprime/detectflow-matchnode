"""
Sigma Matcher Module
Provides Sigma rule matching functionality for security events
"""

from .core import InvertedSignature, Signature, SignatureGroup
from .helpers import (
    SCHEMA_PARSER_ERROR_FIELD,
    ProcessEventsResult,
    convert_events_to_df,
    prepare_df,
    process_events,
)
from .sigma_parser import Sigma, SigmaNotSupported

__all__ = [
    "Signature",
    "SignatureGroup",
    "InvertedSignature",
    "Sigma",
    "SigmaNotSupported",
    "process_events",
    "ProcessEventsResult",
    "prepare_df",
    "convert_events_to_df",
    "SCHEMA_PARSER_ERROR_FIELD",
]
