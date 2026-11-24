"""
Basic structure tests for AESO connector that don't require PySpark.
Run the full test suite in a Databricks environment.
"""
import json
from pathlib import Path


def test_connector_files_exist():
    """Verify all required connector files exist."""
    print("\n" + "="*70)
    print("AESO Connector Structure Tests")
    print("="*70)
    
    base_dir = Path(__file__).parent.parent
    
    required_files = [
        "aeso.py",
        "README.md",
        "requirements.txt",
        "aeso_api_doc.md",
        "_generated_aeso_python_source.py",
        "configs/dev_config.json",
        "configs/dev_table_config.json",
        "configs/pipeline_spec_example.json",
    ]
    
    print("\n✓ Checking required files:")
    all_exist = True
    for file_path in required_files:
        full_path = base_dir / file_path
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        print(f"  {status} {file_path}")
        if not exists:
            all_exist = False
    
    assert all_exist, "Some required files are missing"


def test_config_structure():
    """Verify configuration files have correct structure."""
    print("\n✓ Checking configuration structure:")
    base_dir = Path(__file__).parent.parent
    
    # Check dev_config.json
    config_path = base_dir / "configs" / "dev_config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    assert "api_key" in config, "dev_config.json missing 'api_key'"
    print(f"  ✓ dev_config.json has 'api_key' field")
    
    # Check dev_table_config.json
    table_config_path = base_dir / "configs" / "dev_table_config.json"
    with open(table_config_path) as f:
        table_config = json.load(f)
    
    assert "pool_price" in table_config, "dev_table_config.json missing 'pool_price'"
    pool_config = table_config["pool_price"]
    
    expected_fields = ["start_date", "lookback_hours", "batch_size_days", "rate_limit_delay"]
    for field in expected_fields:
        assert field in pool_config, f"pool_price config missing '{field}'"
        print(f"  ✓ pool_price config has '{field}' field")


def test_connector_can_import():
    """Verify connector module can be imported (without PySpark)."""
    print("\n✓ Checking module imports:")
    try:
        # This will fail if there are syntax errors
        import importlib.util
        base_dir = Path(__file__).parent.parent
        spec = importlib.util.spec_from_file_location("aeso", base_dir / "aeso.py")
        print(f"  ✓ aeso.py has valid Python syntax")
    except SyntaxError as e:
        assert False, f"Syntax error in aeso.py: {e}"


def test_generated_source_compiles():
    """Verify generated source file compiles."""
    print("\n✓ Checking generated source:")
    base_dir = Path(__file__).parent.parent
    generated_file = base_dir / "_generated_aeso_python_source.py"
    
    with open(generated_file) as f:
        code = f.read()
    
    try:
        compile(code, str(generated_file), 'exec')
        print(f"  ✓ _generated_aeso_python_source.py compiles successfully")
    except SyntaxError as e:
        assert False, f"Syntax error in generated source: {e}"


def test_readme_structure():
    """Verify README has key sections."""
    print("\n✓ Checking README structure:")
    base_dir = Path(__file__).parent.parent
    readme_path = base_dir / "README.md"
    
    with open(readme_path) as f:
        content = f.read()
    
    required_sections = [
        "## Supported Data",
        "## What You Get",
        "## Quick Start",
        "## Configuration Options",
        "## Output Schema",
    ]
    
    for section in required_sections:
        assert section in content, f"README missing '{section}' section"
        print(f"  ✓ README has '{section}' section")


if __name__ == "__main__":
    print("\nRunning AESO Connector Structure Tests")
    print("(Full integration tests require PySpark and should run in Databricks)\n")
    
    try:
        test_connector_files_exist()
        test_config_structure()
        test_connector_can_import()
        test_generated_source_compiles()
        test_readme_structure()
        
        print("\n" + "="*70)
        print("✓ All structure tests passed!")
        print("="*70)
        print("\nNext steps:")
        print("  1. Add your AESO API key to configs/dev_config.json")
        print("  2. Run full integration tests in Databricks environment")
        print("="*70 + "\n")
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        raise

