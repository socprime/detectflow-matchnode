"""
Sigma Loader for TDM Format
Loads and parses Sigma rules from TDM JSON format
"""

from app.config.logging import get_logger
from app.domain.models.enriched_sigma import EnrichedSigma
from app.domain.sigma_matcher.sigma_parser import Sigma, SigmaNotSupported

logger = get_logger(__name__)

# Track unsupported rules to avoid logging duplicates on every window
_logged_unsupported_rules: set[str] = set()


def load_sigmas_from_tdm_dict(
    rules_data: list[dict],
    exclude_case_ids: set[str] | None = None,
) -> list[EnrichedSigma]:
    """
    Load Sigma rules from TDM format dict (from Kafka broadcast)

    Args:
        rules_data: List of rule dictionaries in TDM format
        exclude_case_ids: Set of case IDs to exclude (optional)

    Returns:
        List of EnrichedSigma objects
    """
    if exclude_case_ids is None:
        exclude_case_ids = set()

    logger.debug("loading rules", rule_count=len(rules_data))

    enriched_sigmas = []
    skipped_count = 0
    error_count = 0

    for idx, rule in enumerate(rules_data):
        try:
            # Extract case information
            case = rule.get("case", {})
            case_id = case.get("id")
            case_name = case.get("name", "Unknown Rule")

            if not case_id:
                logger.warning("rule missing id", index=idx, reason="no case.id")
                skipped_count += 1
                continue

            if case_id in exclude_case_ids:
                logger.debug("rule excluded", case_id=case_id, reason="in_exclude_list")
                skipped_count += 1
                continue

            # Extract sigma information
            sigma_data = rule.get("sigma", {})
            sigma_text = sigma_data.get("text")
            level = sigma_data.get("level", "medium")

            if not sigma_text:
                logger.warning("rule missing sigma text", case_id=case_id)
                skipped_count += 1
                continue

            # Extract techniques
            tags = rule.get("tags", {})
            techniques = tags.get("technique", [])
            if techniques is None:
                techniques = []

            technique_ids = [tech.get("id") for tech in techniques if tech.get("id")]

            # Parse Sigma rule
            sigma = Sigma(
                text=sigma_text,
                case_id=case_id,
                technique_ids=technique_ids,
            )

            # Create enriched sigma
            enriched = EnrichedSigma(
                sigma=sigma,
                case_name=case_name,
                level=level,
                techniques=techniques,
            )

            enriched_sigmas.append(enriched)

        except SigmaNotSupported as e:
            # Only log unsupported rules once to avoid spam on every window
            rule_key = case_id if "case_id" in locals() else f"idx_{idx}"
            if rule_key not in _logged_unsupported_rules:
                logger.warning("rule not supported", rule_key=rule_key, reason=str(e))
                _logged_unsupported_rules.add(rule_key)
            error_count += 1
        except Exception as e:
            logger.exception("rule loading error", index=idx, error=str(e))
            error_count += 1

    logger.info(
        "rules loaded",
        loaded_count=len(enriched_sigmas),
        skipped_count=skipped_count,
        error_count=error_count,
    )

    return enriched_sigmas
