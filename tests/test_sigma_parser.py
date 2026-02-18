import pytest

from app.domain.sigma_matcher.core import InvertedSignature, SignatureGroup
from app.domain.sigma_matcher.sigma_parser import Sigma, SigmaNotSupported


# Test fixtures
@pytest.fixture
def simple_sigma_yaml():
    return """
title: Test Sigma Rule
id: TEST001
status: test
description: Test rule
author: Test Author
date: 2023/01/01
detection:
    selection:
        EventID: 4624
        TargetUserName: 'SYSTEM'
    condition: selection
"""


@pytest.fixture
def complex_sigma_yaml():
    return """
title: Complex Test Sigma Rule
id: TEST002
status: test
description: Complex test rule
author: Test Author
date: 2023/01/01
detection:
    process_creation:
        EventID: 4688
        NewProcessName|endswith:
            - '\\cmd.exe'
            - '\\powershell.exe'
    network_connection:
        EventID: 3
        DestinationPort: 445
    condition: process_creation and not network_connection
"""


@pytest.fixture
def all_of_sigma_yaml():
    return """
title: All Of Test Sigma Rule
id: TEST003
status: test
description: All of test rule
author: Test Author
date: 2023/01/01
detection:
    process1:
        EventID: 4688
        ProcessName: 'cmd.exe'
    process2:
        EventID: 4688
        ProcessName: 'powershell.exe'
    condition: all of process*
"""


@pytest.fixture
def all_of_sigma_yaml_without_pattern():
    return """
title: All Of Test Sigma Rule
id: TEST003
status: test
description: All of test rule
author: Test Author
date: 2023/01/01
detection:
    selection:
        EventID: 4688
        ProcessName: 'cmd.exe'
    condition: all of selection
"""


@pytest.fixture
def one_of_sigma_yaml():
    return """
title: One Of Test Sigma Rule
id: TEST004
status: test
description: One of test rule
author: Test Author
date: 2023/01/01
detection:
    process1:
        EventID: 4688
        ProcessName: 'cmd.exe'
    process2:
        EventID: 4688
        ProcessName: 'powershell.exe'
    condition: 1 of process*
"""


@pytest.fixture
def one_of_them_yaml():
    return """
title: One Of Them Test Sigma Rule
id: TEST005
status: test
description: One of them test rule
author: Test Author
date: 2023/01/01
detection:
    process1:
        EventID: 4688
        ProcessName: 'cmd.exe'
    process2:
        EventID: 4688
        ProcessName: 'powershell.exe'
    condition: 1 of them
"""


# Test Sigma initialization
def test_sigma_init_valid():
    sigma = Sigma(
        text="detection:\n  condition: selection\n  selection:\n    EventID: 4624",
        case_id="test",
        technique_ids=["T1234"],
    )
    assert sigma.case_id == "test"
    assert sigma.technique_ids == ["T1234"]


def test_sigma_init_invalid_technique_ids():
    with pytest.raises(AssertionError):
        Sigma(text="test", case_id="test", technique_ids="T1234")


def test_sigma_init_invalid_technique_id_type():
    with pytest.raises(AssertionError):
        Sigma(text="test", case_id="test", technique_ids=["T1234", 1234])


# Test Sigma parsing methods
def test_parse_query_simple(simple_sigma_yaml):
    sigma = Sigma(text=simple_sigma_yaml, case_id="TEST001", technique_ids=["T1234"])
    query = sigma.query
    assert isinstance(query, SignatureGroup)
    assert len(query.signatures) == 1

    sig_group = query.signatures[0]
    assert isinstance(sig_group, SignatureGroup)
    assert sig_group.operator == "and"

    signatures = sig_group.signatures
    assert len(signatures) == 2

    event_id_sig = next((s for s in signatures if s.field == "EventID"), None)
    assert event_id_sig is not None
    assert event_id_sig.values == ["4624"]

    username_sig = next((s for s in signatures if s.field == "TargetUserName"), None)
    assert username_sig is not None
    assert username_sig.values == ["SYSTEM"]


def test_parse_query_complex(complex_sigma_yaml):
    sigma = Sigma(text=complex_sigma_yaml, case_id="TEST002", technique_ids=["T1234"])
    query = sigma.query
    assert isinstance(query, SignatureGroup) and len(query.signatures) == 1
    main_group = query.signatures[0]
    assert isinstance(main_group, SignatureGroup)
    assert main_group.operator == "and"
    assert len(main_group.signatures) == 2
    group1 = main_group.signatures[0]
    assert isinstance(group1, SignatureGroup)
    assert group1.operator == "and"
    assert len(group1.signatures) == 2
    assert group1.signatures[0].field == "EventID"
    assert group1.signatures[0].values == ["4688"]
    assert group1.signatures[1].field == "NewProcessName"
    assert group1.signatures[1].values == ["\\cmd.exe", "\\powershell.exe"]
    group2 = main_group.signatures[1]
    assert isinstance(group2, InvertedSignature)
    assert isinstance(group2.sig, SignatureGroup)
    assert group2.sig.operator == "and"
    assert len(group2.sig.signatures) == 2
    assert group2.sig.signatures[0].field == "EventID"
    assert group2.sig.signatures[0].values == ["3"]
    assert group2.sig.signatures[1].field == "DestinationPort"
    assert group2.sig.signatures[1].values == ["445"]


def test_parse_query_all_of(all_of_sigma_yaml):
    sigma = Sigma(text=all_of_sigma_yaml, case_id="TEST003", technique_ids=["T1234"])
    query = sigma.query

    assert isinstance(query, SignatureGroup) and len(query.signatures) == 1
    main_group = query.signatures[0]
    assert isinstance(main_group, SignatureGroup)
    assert main_group.operator == "and"
    assert len(main_group.signatures) == 2

    group1 = main_group.signatures[0]
    assert isinstance(group1, SignatureGroup)
    assert group1.operator == "and"
    assert len(group1.signatures) == 2
    assert group1.signatures[0].field == "EventID"
    assert group1.signatures[0].values == ["4688"]
    assert group1.signatures[1].field == "ProcessName"
    assert group1.signatures[1].values == ["cmd.exe"]

    group2 = main_group.signatures[1]
    assert isinstance(group2, SignatureGroup)
    assert group2.operator == "and"
    assert len(group2.signatures) == 2
    assert group2.signatures[0].field == "EventID"
    assert group2.signatures[0].values == ["4688"]
    assert group2.signatures[1].field == "ProcessName"
    assert group2.signatures[1].values == ["powershell.exe"]


def test_parse_query_one_of(one_of_sigma_yaml):
    sigma = Sigma(text=one_of_sigma_yaml, case_id="TEST004", technique_ids=["T1234"])
    query = sigma.query

    assert isinstance(query, SignatureGroup) and len(query.signatures) == 1
    main_group = query.signatures[0]
    assert isinstance(main_group, SignatureGroup)
    assert main_group.operator == "or"
    assert len(main_group.signatures) == 2

    group1 = main_group.signatures[0]
    assert isinstance(group1, SignatureGroup)
    assert group1.operator == "and"
    assert len(group1.signatures) == 2
    assert group1.signatures[0].field == "EventID"
    assert group1.signatures[0].values == ["4688"]
    assert group1.signatures[1].field == "ProcessName"
    assert group1.signatures[1].values == ["cmd.exe"]

    group2 = main_group.signatures[1]
    assert isinstance(group2, SignatureGroup)
    assert group2.operator == "and"
    assert len(group2.signatures) == 2
    assert group2.signatures[0].field == "EventID"
    assert group2.signatures[0].values == ["4688"]
    assert group2.signatures[1].field == "ProcessName"
    assert group2.signatures[1].values == ["powershell.exe"]


def test_parse_query_one_them(one_of_them_yaml):
    sigma = Sigma(text=one_of_them_yaml, case_id="TEST004", technique_ids=["T1234"])
    query = sigma.query

    assert isinstance(query, SignatureGroup) and len(query.signatures) == 1
    main_group = query.signatures[0]
    assert isinstance(main_group, SignatureGroup)
    assert main_group.operator == "or"
    assert len(main_group.signatures) == 2

    group1 = main_group.signatures[0]
    assert isinstance(group1, SignatureGroup)
    assert group1.operator == "and"
    assert len(group1.signatures) == 2
    assert group1.signatures[0].field == "EventID"
    assert group1.signatures[0].values == ["4688"]
    assert group1.signatures[1].field == "ProcessName"
    assert group1.signatures[1].values == ["cmd.exe"]

    group2 = main_group.signatures[1]
    assert isinstance(group2, SignatureGroup)
    assert group2.operator == "and"
    assert len(group2.signatures) == 2
    assert group2.signatures[0].field == "EventID"
    assert group2.signatures[0].values == ["4688"]
    assert group2.signatures[1].field == "ProcessName"
    assert group2.signatures[1].values == ["powershell.exe"]


def test_get_detection_invalid_yaml():
    with pytest.raises(SigmaNotSupported):
        sigma = Sigma(text="invalid: yaml: :", case_id="test", technique_ids=["T1234"])
        sigma._get_detection_from_sigma()


def test_get_operator():
    assert Sigma._get_operator(set()) == "or"
    assert Sigma._get_operator({"all"}) == "and"
    modifiers = {"all", "endswith"}
    result = Sigma._get_operator(modifiers)
    assert result == "and"
    assert modifiers == {"endswith"}


def test_get_command():
    assert Sigma._get_command(set()) == "equals"
    assert Sigma._get_command({"contains"}) == "contains"
    assert Sigma._get_command({"endswith"}) == "endswith"
    assert Sigma._get_command({"startswith"}) == "startswith"

    with pytest.raises(SigmaNotSupported):
        Sigma._get_command({"unknown"})


def test_process_detection_values_simple_dict():
    """Test processing a simple dictionary with various value types."""
    input_data = {
        "bool_true": True,
        "bool_false": False,
        "float_value": 1.5,
        "int_value": 10,
        "str_value": "test",
        "none_value": None,
    }

    expected = {
        "bool_true": "true",
        "bool_false": "false",
        "float_value": "1.5",
        "int_value": "10",
        "str_value": "test",
        "none_value": None,
    }

    result = Sigma._process_detection_values(input_data)
    assert result == expected


def test_process_detection_values_simple_list():
    """Test processing a simple list with various value types."""
    input_data = [True, False, 1.5, 10, "test", None]

    expected = ["true", "false", "1.5", "10", "test", None]

    result = Sigma._process_detection_values(input_data)
    assert result == expected


def test_process_detection_values_nested_structures():
    """Test processing nested dictionaries and lists."""
    input_data = {
        "dict": {"bool_value": True, "float_value": 2.5},
        "list": [False, 3.5, "string"],
        "mixed": {"nested_list": [True, 4.5, {"inner": False}]},
    }

    expected = {
        "dict": {"bool_value": "true", "float_value": "2.5"},
        "list": ["false", "3.5", "string"],
        "mixed": {"nested_list": ["true", "4.5", {"inner": "false"}]},
    }

    result = Sigma._process_detection_values(input_data)
    assert result == expected


def test_process_detection_values_primitive_types():
    """Test processing primitive types."""
    assert Sigma._process_detection_values(10) == "10"
    assert Sigma._process_detection_values("string") == "string"
    assert Sigma._process_detection_values(None) is None
    assert Sigma._process_detection_values(True) == "true"
    assert Sigma._process_detection_values(1.5) == "1.5"


def test_process_detection_values_detection_example():
    """Test processing a realistic detection example."""
    detection = {
        "selection": {
            "EventID": 4624,
            "LogonType": 3,
            "TargetUserName": "Admin",
            "Enabled": True,
            "Score": 0.95,
        },
        "condition": "selection",
    }

    expected = {
        "selection": {
            "EventID": "4624",
            "LogonType": "3",
            "TargetUserName": "Admin",
            "Enabled": "true",
            "Score": "0.95",
        },
        "condition": "selection",
    }

    result = Sigma._process_detection_values(detection)
    assert result == expected


def test_widcards_does_not_support_endswith_startswith_modifiers():
    # mocker.patch("src.core.get_field_name_in_event", side_effect=mocked_get_field_name_in_event, autospec=True)

    sigma_text = """
detection:
    condition: selection
    selection:
        field1|startswith: a*c
"""
    with pytest.raises(SigmaNotSupported):
        Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])

    sigma_text = """
detection:
    condition: selection
    selection:
        field1|endswith: a?c
"""
    with pytest.raises(SigmaNotSupported):
        Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])

    sigma_text = r"""
detection:
    condition: selection
    selection:
        field1|endswith: a\*c
"""
    Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])


def test_all_of_should_be_pattern(all_of_sigma_yaml_without_pattern):
    with pytest.raises(SigmaNotSupported):
        Sigma(text=all_of_sigma_yaml_without_pattern, case_id="TEST003", technique_ids=["T1234"])
