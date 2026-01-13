"""
Structural tests for Petrinex connector (no PySpark required)
"""
import os
import sys
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_files_exist():
    """Test that all required files exist"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    required_files = [
        "petrinex.py",
        "README.md",
        "petrinex_api_doc.md",
        "requirements.txt",
        "configs/dev_config.json",
        "configs/dev_table_config.json",
        "configs/pipeline_spec_example.json",
    ]
    
    for file_path in required_files:
        full_path = os.path.join(base_dir, file_path)
        assert os.path.exists(full_path), f"Missing required file: {file_path}"
    
    print("✓ All required files exist")


def test_config_json_valid():
    """Test that all JSON config files are valid"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    json_files = [
        "configs/dev_config.json",
        "configs/dev_table_config.json",
        "configs/pipeline_spec_example.json",
    ]
    
    for file_path in json_files:
        full_path = os.path.join(base_dir, file_path)
        with open(full_path, 'r') as f:
            try:
                json.load(f)
            except json.JSONDecodeError as e:
                raise AssertionError(f"Invalid JSON in {file_path}: {e}")
    
    print("✓ All JSON files are valid")


def test_python_syntax():
    """Test that Python files have valid syntax"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    python_files = [
        "petrinex.py",
    ]
    
    for file_path in python_files:
        full_path = os.path.join(base_dir, file_path)
        with open(full_path, 'r') as f:
            code = f.read()
            try:
                compile(code, full_path, 'exec')
            except SyntaxError as e:
                raise AssertionError(f"Syntax error in {file_path}: {e}")
    
    print("✓ All Python files have valid syntax")


def test_connector_class_exists():
    """Test that LakeflowConnect class exists with required methods"""
    import petrinex
    
    assert hasattr(petrinex, 'LakeflowConnect'), "LakeflowConnect class not found"
    
    connector_class = petrinex.LakeflowConnect
    required_methods = [
        'list_tables',
        'get_table_schema',
        'read_table_metadata',
        'read_table',
    ]
    
    for method in required_methods:
        assert hasattr(connector_class, method), f"Missing method: {method}"
    
    print("✓ LakeflowConnect class exists with all required methods")


def test_supported_tables():
    """Test that supported tables are defined"""
    import petrinex
    
    assert hasattr(petrinex, 'SUPPORTED_TABLES'), "SUPPORTED_TABLES not found"
    assert len(petrinex.SUPPORTED_TABLES) > 0, "SUPPORTED_TABLES is empty"
    assert "volumetrics" in petrinex.SUPPORTED_TABLES, "volumetrics table not in SUPPORTED_TABLES"
    
    print(f"✓ Supported tables: {petrinex.SUPPORTED_TABLES}")


if __name__ == "__main__":
    print("\nRunning Petrinex connector structural tests...\n")
    
    try:
        test_files_exist()
        test_config_json_valid()
        test_python_syntax()
        test_connector_class_exists()
        test_supported_tables()
        
        print("\n✅ All structural tests passed!\n")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}\n")
        sys.exit(1)

