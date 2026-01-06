#!/usr/bin/env python3
"""
Basic structure test for AESO connector (no PySpark required).

This test verifies the connector module structure and basic configuration
without importing PySpark, which can cause segfaults on some macOS environments.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))


def test_module_imports():
    """Test that we can import the connector module."""
    try:
        # Import without initializing (no PySpark import)
        import sources.aeso
        print("✓ Module import successful")
        return True
    except Exception as e:
        print(f"✗ Module import failed: {e}")
        return False


def test_config_files_exist():
    """Test that configuration files exist."""
    import os
    from pathlib import Path
    
    aeso_dir = Path(__file__).parent.parent
    
    files_to_check = [
        "aeso.py",
        "README.md",
        "requirements.txt",
        "aeso_api_doc.md",
        "configs/dev_config.json",
        "configs/example_configs.json",
        "configs/pipeline_spec_example.json",
    ]
    
    all_exist = True
    for file_path in files_to_check:
        full_path = aeso_dir / file_path
        if full_path.exists():
            print(f"✓ {file_path} exists")
        else:
            print(f"✗ {file_path} missing")
            all_exist = False
    
    return all_exist


def test_requirements_file():
    """Test that requirements.txt has necessary dependencies."""
    from pathlib import Path
    
    req_file = Path(__file__).parent.parent / "requirements.txt"
    
    if not req_file.exists():
        print("✗ requirements.txt not found")
        return False
    
    content = req_file.read_text()
    required_deps = ["aeso-python-api", "requests", "pyspark"]
    
    all_found = True
    for dep in required_deps:
        if dep in content:
            print(f"✓ {dep} in requirements.txt")
        else:
            print(f"✗ {dep} missing from requirements.txt")
            all_found = False
    
    return all_found


def test_readme_structure():
    """Test that README has key sections."""
    from pathlib import Path
    
    readme = Path(__file__).parent.parent / "README.md"
    
    if not readme.exists():
        print("✗ README.md not found")
        return False
    
    content = readme.read_text()
    required_sections = [
        "Quick Start",
        "Configuration Options",
        "Output Schema",
        "Troubleshooting",
    ]
    
    all_found = True
    for section in required_sections:
        if section in content:
            print(f"✓ README has '{section}' section")
        else:
            print(f"✗ README missing '{section}' section")
            all_found = False
    
    return all_found


def test_api_doc_exists():
    """Test that API documentation exists."""
    from pathlib import Path
    
    api_doc = Path(__file__).parent.parent / "aeso_api_doc.md"
    
    if not api_doc.exists():
        print("✗ aeso_api_doc.md not found")
        return False
    
    content = api_doc.read_text()
    
    # Check for key sections
    required_sections = [
        "Authorization",
        "Object List",
        "Object Schema",
        "pool_price",
        "Read API",
    ]
    
    all_found = True
    for section in required_sections:
        if section in content:
            print(f"✓ API doc has '{section}' section")
        else:
            print(f"✗ API doc missing '{section}' section")
            all_found = False
    
    return all_found


if __name__ == "__main__":
    print("=" * 70)
    print("AESO Connector Basic Structure Tests")
    print("(No PySpark required)")
    print("=" * 70)
    print()
    
    tests = [
        ("Module Imports", test_module_imports),
        ("Config Files", test_config_files_exist),
        ("Requirements File", test_requirements_file),
        ("README Structure", test_readme_structure),
        ("API Documentation", test_api_doc_exists),
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\n{name}:")
        print("-" * 70)
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"✗ Test error: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    print()
    print("=" * 70)
    print("Summary:")
    print("=" * 70)
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")
    
    print()
    print(f"Result: {passed_count}/{total_count} tests passed")
    print("=" * 70)
    
    sys.exit(0 if passed_count == total_count else 1)

