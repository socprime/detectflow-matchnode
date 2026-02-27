from typing import Any

from pydantic import BaseModel, ValidationError

from app.config.logging import get_logger
from app.domain.sigma_matcher.sigma_parser import Sigma, SigmaNotSupported

logger = get_logger(__name__)

# Track unsupported rules to avoid logging duplicates on every window
_logged_unsupported_rules: set[str] = set()


class CaseDict(BaseModel, extra="ignore"):
    id: str
    name: str | None = None


class SigmaDict(BaseModel, extra="ignore"):
    text: str
    level: str | None = None


class TechniqueDict(BaseModel, extra="ignore"):
    id: str | None = None
    name: str | None = None


class TagsDict(BaseModel, extra="ignore"):
    technique: list[TechniqueDict] | None = None


class RuleData(BaseModel, extra="ignore"):
    case: CaseDict
    sigma: SigmaDict
    tags: Any | None = None


def _parse_rule_data(rule_dict: dict) -> RuleData | None:
    try:
        return RuleData.model_validate(rule_dict)
    except ValidationError as e:
        logger.warning("rule data validation error", error=str(e))
        return None


def _parse_techniques(rule_dict: dict) -> list[dict]:
    """Parse optional metadata safely; invalid metadata does not invalidate the rule."""
    try:
        tags = TagsDict.model_validate(rule_dict.get("tags") or {})
        return [x.model_dump() for x in (tags.technique or [])]
    except ValidationError as e:
        logger.warning("rule metadata validation error", error=str(e))
        return []


def load_sigmas_from_rules_data(rules_data: list[dict]) -> list[Sigma]:
    """
    Load Sigma rules from rules data (from Kafka broadcast)

    Args:
        rules_data: List of rule dictionaries

    Returns:
        List of Sigma objects
    """
    logger.debug("loading rules", rule_count=len(rules_data))

    sigmas = []
    unsupported_count = 0
    error_count = 0

    for d in rules_data:
        try:
            rule = _parse_rule_data(d)
            if not rule:
                error_count += 1
                continue
            if not rule.case.id:
                logger.warning("rule missing case id")
                error_count += 1
                continue
            if not rule.sigma.text:
                logger.warning("rule missing sigma text")
                error_count += 1
                continue

            sigma = Sigma(
                text=rule.sigma.text,
                case_id=rule.case.id,
                title=rule.case.name,
                level=rule.sigma.level,
                techniques=_parse_techniques(d),
            )

            sigmas.append(sigma)

        except SigmaNotSupported as e:
            rule_key = rule.case.id
            if rule_key and rule_key not in _logged_unsupported_rules:
                logger.warning("rule not supported", rule_key=rule_key, reason=str(e))
                _logged_unsupported_rules.add(rule_key)
            unsupported_count += 1
        except Exception as e:
            logger.exception("rule loading error", error=str(e))
            error_count += 1

    if rules_data and not sigmas:
        raise ValueError("No valid rules loaded")

    logger.info(
        "rules loaded",
        loaded_count=len(sigmas),
        unsupported_count=unsupported_count,
        error_count=error_count,
    )

    return sigmas
