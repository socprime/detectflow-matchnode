from app.domain.rules.loader import load_sigmas_from_rules_data

VALID_SIGMA_TEXT = """detection:
  condition: selection
  selection:
    EventID: 4624
"""


def test_load_rules_allows_null_technique_list():
    rules_data = [
        {
            "case": {"id": "rule-1", "name": "Rule 1"},
            "sigma": {"text": VALID_SIGMA_TEXT, "level": "medium"},
            "tags": {"technique": None},
        }
    ]

    sigmas = load_sigmas_from_rules_data(rules_data)

    assert len(sigmas) == 1
    assert sigmas[0].case_id == "rule-1"
    assert sigmas[0].techniques == []
    assert sigmas[0].technique_ids == []


def test_load_rules_allows_techniques_without_id():
    rules_data = [
        {
            "case": {"id": "rule-1", "name": "Rule 1"},
            "sigma": {"text": VALID_SIGMA_TEXT, "level": "medium"},
            "tags": {
                "technique": [
                    {"name": "No ID technique"},
                    {"id": "T1003", "name": "Credential Dumping"},
                ]
            },
        }
    ]

    sigmas = load_sigmas_from_rules_data(rules_data)

    assert len(sigmas) == 1
    assert sigmas[0].technique_ids == ["T1003"]


def test_load_rules_allows_missing_tags():
    rules_data = [
        {
            "case": {"id": "rule-1", "name": "Rule 1"},
            "sigma": {"text": VALID_SIGMA_TEXT, "level": "medium"},
        }
    ]

    sigmas = load_sigmas_from_rules_data(rules_data)

    assert len(sigmas) == 1
    assert sigmas[0].techniques == []
    assert sigmas[0].technique_ids == []


def test_load_rules_allows_invalid_metadata_and_keeps_valid_core_fields():
    rules_data = [
        {
            "case": {"id": "invalid-metadata", "name": "Invalid metadata shape"},
            "sigma": {"text": VALID_SIGMA_TEXT, "level": "medium"},
            "tags": {"technique": "not-a-list"},
        },
        {
            "case": {"id": "rule-2", "name": "Rule 2"},
            "sigma": {"text": VALID_SIGMA_TEXT, "level": "high"},
            "tags": {"technique": [{"id": "T1059"}]},
        },
    ]

    sigmas = load_sigmas_from_rules_data(rules_data)

    assert len(sigmas) == 2

    sigma_by_case_id = {s.case_id: s for s in sigmas}
    assert set(sigma_by_case_id) == {"invalid-metadata", "rule-2"}

    assert sigma_by_case_id["invalid-metadata"].techniques == []
    assert sigma_by_case_id["invalid-metadata"].technique_ids == []
    assert sigma_by_case_id["rule-2"].technique_ids == ["T1059"]


def test_load_rules_returns_empty_for_empty_input():
    assert load_sigmas_from_rules_data([]) == []


def test_load_rules_returns_empty_when_no_valid_sigmas():
    rules_data = [
        {
            "case": {"id": "rule-1", "name": "Rule 1"},
            "sigma": {"text": "invalid: yaml: :"},
        }
    ]

    assert load_sigmas_from_rules_data(rules_data) == []
