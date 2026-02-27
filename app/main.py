#!/usr/bin/env python3
"""
Apache Flink Sigma Detector
Entry point for the Flink job with CLI arguments support for multi-tenancy
"""

import argparse
import os

from dotenv import load_dotenv

from app.jobs.sigma_detection import run_sigma_detection_job

# Load environment variables from .env file for local development. In container, file .env is not present.
load_dotenv()


def main():
    """
    Entry point with CLI arguments

    Supports multi-tenancy by accepting job-specific parameters:
    - job_id: Unique identifier for filtering rules
    - input_topic/output_topic: Per-job Kafka topics
    - output_mode: Control whether to emit all events or only matched

    These arguments override environment variables when provided.
    """
    parser = argparse.ArgumentParser(
        description="Apache Flink Sigma Detection Job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Job identity (required for multi-tenancy)
    parser.add_argument(
        "--job-id",
        type=str,
        default=os.getenv("JOB_ID"),
        help="Unique job identifier for filtering rules (required for multi-tenancy)",
    )

    # Kafka topics (override env vars if provided)
    parser.add_argument(
        "--input-topics",
        type=str,
        default=os.getenv("KAFKA_INPUT_TOPICS"),
        help="Kafka topics with input events (comma-separated, e.g. 'topic1,topic2,topic3')",
    )
    parser.add_argument(
        "--output-topic",
        type=str,
        default=os.getenv("KAFKA_OUTPUT_TOPIC"),
        help="Kafka topic for output detections",
    )
    parser.add_argument(
        "--rules-topic",
        type=str,
        default=os.getenv("KAFKA_RULES_TOPIC"),
        help="Kafka topic with Sigma rules (shared across jobs, filtered by job_id)",
    )
    parser.add_argument(
        "--metrics-topic",
        type=str,
        default=os.getenv("KAFKA_METRICS_TOPIC"),
        help="Kafka topic for per-rule metrics (optional, empty = disabled)",
    )

    # Output behavior
    parser.add_argument(
        "--output-mode",
        type=str,
        choices=["all_events", "matched_only"],
        default=os.getenv("OUTPUT_MODE"),
        help="Output all events or only matched events",
    )

    parser.add_argument(
        "--keep-filtered-events",
        action="store_true",
        default=None,
        help="Keep prefiltered events in output as unmatched (default: False, i.e. drop them)",
    )

    parser.add_argument(
        "--apply-parser-to-output-events",
        action="store_true",
        default=os.getenv("APPLY_PARSER_TO_OUTPUT_EVENTS", "").lower() in ("true", "1"),
        help="Apply parser to output events",
    )

    args = parser.parse_args()

    # Validate required arguments
    if not args.job_id:
        parser.error("--job-id is required (or set JOB_ID environment variable)")
    if not args.input_topics:
        parser.error("--input-topics is required (or set KAFKA_INPUT_TOPICS environment variable)")
    if not args.output_topic:
        parser.error("--output-topic is required (or set KAFKA_OUTPUT_TOPIC environment variable)")

    # Parse comma-separated topics into list
    input_topics = [t.strip() for t in args.input_topics.split(",") if t.strip()]
    if not input_topics:
        parser.error("--input-topics must contain at least one topic")

    # Run Flink job with provided parameters
    run_sigma_detection_job(
        job_id=args.job_id,
        input_topics=input_topics,
        output_topic=args.output_topic,
        rules_topic=args.rules_topic,
        output_mode=args.output_mode,
        keep_filtered_events=args.keep_filtered_events,
        metrics_topic=args.metrics_topic,
        apply_parser_to_output_events=args.apply_parser_to_output_events,
    )


if __name__ == "__main__":
    main()
