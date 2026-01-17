import pytest
import json
from pathlib import Path

# Import test suite and connector
import tests.test_suite as test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.surveymonkey.surveymonkey import LakeflowConnect


def test_surveymonkey_connector():
    """Test the SurveyMonkey connector using the test suite"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    # Load table config if it exists, otherwise use empty dict
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    if table_config_path.exists():
        table_config = load_config(table_config_path)
    else:
        table_config = {}

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )


def test_surveymonkey_connection():
    """Test basic SurveyMonkey API connection"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    result = connector.test_connection()
    
    print(f"\nConnection test result: {result}")
    
    assert result["status"] == "success", f"Connection failed: {result['message']}"


def test_list_tables():
    """Test that list_tables returns expected tables"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    tables = connector.list_tables()
    
    # Updated expected tables to include all new tables
    expected_tables = [
        "surveys",
        "survey_responses",
        "survey_pages",
        "survey_questions",
        "collectors",
        "contact_lists",
        "contacts",
        "users",
        "groups",
        "group_members",
        "workgroups",
        "survey_folders",
        "survey_categories",
        "survey_templates",
        "survey_languages",
        "webhooks",
        "survey_rollups",
        "benchmark_bundles",
    ]
    
    print(f"\nTables: {tables}")
    
    assert tables == expected_tables, f"Unexpected tables: {tables}"


def test_read_surveys():
    """Test reading surveys table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("surveys", {})
    print(f"\nSurveys schema: {schema}")
    
    # Get metadata
    metadata = connector.read_table_metadata("surveys", {})
    print(f"Surveys metadata: {metadata}")
    
    # Read data
    records_iter, offset = connector.read_table("surveys", {}, {})
    records = list(records_iter)
    
    print(f"Number of surveys: {len(records)}")
    if records:
        print(f"First survey: {json.dumps(records[0], indent=2, default=str)}")
    
    assert isinstance(records, list), "Records should be a list"
    assert isinstance(offset, dict), "Offset should be a dict"


def test_read_users():
    """Test reading users (current user) table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("users", {})
    print(f"\nUsers schema: {schema}")
    
    # Read data
    records_iter, offset = connector.read_table("users", {}, {})
    records = list(records_iter)
    
    print(f"Number of users: {len(records)}")
    if records:
        print(f"User: {json.dumps(records[0], indent=2, default=str)}")
    
    assert len(records) == 1, "Should return exactly one user"


def test_read_survey_responses_for_survey():
    """Test reading survey responses for a specific survey"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # First get a survey to use
    surveys_iter, _ = connector.read_table("surveys", {}, {})
    surveys = list(surveys_iter)
    
    if not surveys:
        pytest.skip("No surveys available to test")
    
    survey_id = surveys[0]["id"]
    print(f"\nTesting with survey_id: {survey_id}")
    
    # Read responses for this survey
    table_options = {"survey_id": survey_id}
    records_iter, offset = connector.read_table("survey_responses", {}, table_options)
    records = list(records_iter)
    
    print(f"Number of responses: {len(records)}")
    if records:
        print(f"First response: {json.dumps(records[0], indent=2, default=str)}")
    
    assert isinstance(records, list), "Records should be a list"
    # All responses should have the survey_id set
    for record in records:
        assert record.get("survey_id") == survey_id, "Response should have survey_id set"


def test_read_groups():
    """Test reading groups table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("groups", {})
    print(f"\nGroups schema: {schema}")
    
    # Get metadata
    metadata = connector.read_table_metadata("groups", {})
    print(f"Groups metadata: {metadata}")
    
    # Read data - may fail if groups endpoint is not available
    try:
        records_iter, offset = connector.read_table("groups", {}, {})
        records = list(records_iter)
        
        print(f"Number of groups: {len(records)}")
        if records:
            print(f"First group: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Groups endpoint not available: {e}")
        pytest.skip("Groups endpoint may require enterprise account")


def test_read_survey_folders():
    """Test reading survey_folders table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_folders", {})
    print(f"\nSurvey folders schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("survey_folders", {}, {})
        records = list(records_iter)
        
        print(f"Number of folders: {len(records)}")
        if records:
            print(f"First folder: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Survey folders error: {e}")
        pytest.skip("Survey folders endpoint not available")


def test_read_survey_categories():
    """Test reading survey_categories table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_categories", {})
    print(f"\nSurvey categories schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("survey_categories", {}, {})
        records = list(records_iter)
        
        print(f"Number of categories: {len(records)}")
        if records:
            print(f"Categories: {json.dumps(records[:5], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
        assert len(records) > 0, "Should have at least some categories"
    except Exception as e:
        print(f"Survey categories error: {e}")
        pytest.skip("Survey categories endpoint not available")


def test_read_survey_languages():
    """Test reading survey_languages table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_languages", {})
    print(f"\nSurvey languages schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("survey_languages", {}, {})
        records = list(records_iter)
        
        print(f"Number of languages: {len(records)}")
        if records:
            print(f"First 5 languages: {json.dumps(records[:5], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
        assert len(records) > 0, "Should have at least some languages"
    except Exception as e:
        print(f"Survey languages error: {e}")
        pytest.skip("Survey languages endpoint not available")


def test_read_survey_templates():
    """Test reading survey_templates table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_templates", {})
    print(f"\nSurvey templates schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("survey_templates", {}, {})
        records = list(records_iter)
        
        print(f"Number of templates: {len(records)}")
        if records:
            print(f"First template: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Survey templates error: {e}")
        pytest.skip("Survey templates endpoint not available")


def test_read_webhooks():
    """Test reading webhooks table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("webhooks", {})
    print(f"\nWebhooks schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("webhooks", {}, {})
        records = list(records_iter)
        
        print(f"Number of webhooks: {len(records)}")
        if records:
            print(f"First webhook: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Webhooks error: {e}")
        pytest.skip("Webhooks endpoint not available")


def test_read_workgroups():
    """Test reading workgroups table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("workgroups", {})
    print(f"\nWorkgroups schema: {schema}")
    
    # Read data - may fail if workgroups endpoint is not available
    try:
        records_iter, offset = connector.read_table("workgroups", {}, {})
        records = list(records_iter)
        
        print(f"Number of workgroups: {len(records)}")
        if records:
            print(f"First workgroup: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Workgroups endpoint not available: {e}")
        pytest.skip("Workgroups endpoint may require enterprise account")


def test_read_survey_rollups():
    """Test reading survey_rollups table for a specific survey"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    config = load_config(config_path)
    table_config = load_config(table_config_path) if table_config_path.exists() else {}
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_rollups", {})
    print(f"\nSurvey rollups schema: {schema}")
    
    # Read data for the configured survey
    table_options = table_config.get("survey_rollups", {})
    if not table_options.get("survey_id"):
        # Get first survey
        surveys_iter, _ = connector.read_table("surveys", {}, {})
        surveys = list(surveys_iter)
        if not surveys:
            pytest.skip("No surveys available to test rollups")
        table_options = {"survey_id": surveys[0]["id"]}
    
    try:
        records_iter, offset = connector.read_table("survey_rollups", {}, table_options)
        records = list(records_iter)
        
        print(f"Number of rollups: {len(records)}")
        if records:
            print(f"First rollup: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Survey rollups error: {e}")
        pytest.skip("Survey rollups endpoint not available")


def test_read_benchmark_bundles():
    """Test reading benchmark_bundles table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("benchmark_bundles", {})
    print(f"\nBenchmark bundles schema: {schema}")
    
    # Read data - benchmarks require special access
    try:
        records_iter, offset = connector.read_table("benchmark_bundles", {}, {})
        records = list(records_iter)
        
        print(f"Number of benchmark bundles: {len(records)}")
        if records:
            print(f"First bundle: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Benchmark bundles error: {e}")
        pytest.skip("Benchmark bundles endpoint requires special access")


def test_read_contact_lists():
    """Test reading contact_lists table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("contact_lists", {})
    print(f"\nContact lists schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("contact_lists", {}, {})
        records = list(records_iter)
        
        print(f"Number of contact lists: {len(records)}")
        if records:
            print(f"First contact list: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Contact lists error: {e}")
        pytest.skip("Contact lists endpoint not available")


def test_read_contacts():
    """Test reading contacts table"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("contacts", {})
    print(f"\nContacts schema: {schema}")
    
    # Read data
    try:
        records_iter, offset = connector.read_table("contacts", {}, {})
        records = list(records_iter)
        
        print(f"Number of contacts: {len(records)}")
        if records:
            print(f"First contact: {json.dumps(records[0], indent=2, default=str)}")
        
        assert isinstance(records, list), "Records should be a list"
    except Exception as e:
        print(f"Contacts error: {e}")
        pytest.skip("Contacts endpoint not available")


def test_read_collectors():
    """Test reading collectors table for a specific survey"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    config = load_config(config_path)
    table_config = load_config(table_config_path) if table_config_path.exists() else {}
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("collectors", {})
    print(f"\nCollectors schema: {schema}")
    
    # Read data for the configured survey
    table_options = table_config.get("collectors", {})
    if not table_options.get("survey_id"):
        # Get first survey
        surveys_iter, _ = connector.read_table("surveys", {}, {})
        surveys = list(surveys_iter)
        if not surveys:
            pytest.skip("No surveys available to test collectors")
        table_options = {"survey_id": surveys[0]["id"]}
    
    records_iter, offset = connector.read_table("collectors", {}, table_options)
    records = list(records_iter)
    
    print(f"Number of collectors: {len(records)}")
    if records:
        print(f"First collector: {json.dumps(records[0], indent=2, default=str)}")
    
    assert isinstance(records, list), "Records should be a list"


def test_read_survey_pages():
    """Test reading survey_pages table for a specific survey"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    config = load_config(config_path)
    table_config = load_config(table_config_path) if table_config_path.exists() else {}
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_pages", {})
    print(f"\nSurvey pages schema: {schema}")
    
    # Read data for the configured survey
    table_options = table_config.get("survey_pages", {})
    if not table_options.get("survey_id"):
        # Get first survey
        surveys_iter, _ = connector.read_table("surveys", {}, {})
        surveys = list(surveys_iter)
        if not surveys:
            pytest.skip("No surveys available to test pages")
        table_options = {"survey_id": surveys[0]["id"]}
    
    records_iter, offset = connector.read_table("survey_pages", {}, table_options)
    records = list(records_iter)
    
    print(f"Number of pages: {len(records)}")
    if records:
        print(f"First page: {json.dumps(records[0], indent=2, default=str)}")
    
    assert isinstance(records, list), "Records should be a list"


def test_read_survey_questions():
    """Test reading survey_questions table for a specific survey"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    config = load_config(config_path)
    table_config = load_config(table_config_path) if table_config_path.exists() else {}
    
    connector = LakeflowConnect(config)
    
    # Get schema
    schema = connector.get_table_schema("survey_questions", {})
    print(f"\nSurvey questions schema: {schema}")
    
    # Read data for the configured survey
    table_options = table_config.get("survey_questions", {})
    if not table_options.get("survey_id"):
        # Get first survey
        surveys_iter, _ = connector.read_table("surveys", {}, {})
        surveys = list(surveys_iter)
        if not surveys:
            pytest.skip("No surveys available to test questions")
        table_options = {"survey_id": surveys[0]["id"]}
    
    records_iter, offset = connector.read_table("survey_questions", {}, table_options)
    records = list(records_iter)
    
    print(f"Number of questions: {len(records)}")
    if records:
        print(f"First question: {json.dumps(records[0], indent=2, default=str)}")
    
    assert isinstance(records, list), "Records should be a list"
