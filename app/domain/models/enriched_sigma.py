"""
Core domain models
"""

from app.domain.sigma_matcher.sigma_parser import Sigma


class EnrichedSigma:
    """
    Extended Sigma object with TDM metadata
    """

    def __init__(
        self,
        sigma: Sigma,
        case_name: str,
        level: str,
        techniques: list[dict],
    ):
        self.sigma = sigma
        self.case_id = sigma.case_id
        self.case_name = case_name
        self.level = level
        self.techniques = (
            techniques  # [{"id": "T1003.001", "name": "LSASS Memory", "tactics": [...]}]
        )
        self.technique_ids = sigma.technique_ids
        self.query = sigma.query
        self.title = sigma.title

    def __repr__(self):
        return (
            f"EnrichedSigma(case_id={self.case_id}, case_name={self.case_name}, level={self.level})"
        )

    def get_enrichment_data(self) -> dict:
        """Get enrichment metadata for matched events"""
        return {
            "rule_id": self.case_id,
            "rule_title": self.case_name or self.title,
            "severity": self.level,
            "technique_ids": self.technique_ids,
            "techniques": self.techniques,
        }
