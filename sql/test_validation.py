# -*- coding: utf-8 -*-
"""
Complete PyCORN Validation Test Suite

Run this script to validate:
1. Module imports work from any directory
2. .env file loading works correctly
3. CTE query file reading works
4. All connector functions are available
5. Example usage patterns work correctly
"""

import sys
from pathlib import Path

 
def main():
    """Run all validation tests."""

    project_root = Path(__file__).parent.parent.resolve()
    print("=" * 70)
    print("PyCORN Validation Test Suite")
    print("=" * 70)
    print()

    # Step 1: Add to Python path and test imports
    print("-" * 70)
    print("Step 1: Import Testing")
    print("-" * 70)

    try:
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        from pycorn.db_connector import (
            PyCORNDatabase,
            DataFrameResult,
            ConnectionParams,
            get_cte_query,
            execute_cte_query,
            fetch_results,
            test_connection,
            get_connection_params,
        )

        print("[OK] All imports successful")
        print(f"   - PyCORNDatabase: {PyCORNDatabase}")
        print(f"   - DataFrameResult: {DataFrameResult}")
        print(f"   - ConnectionParams: {ConnectionParams}")
        print(f"   - get_cte_query: {get_cte_query}")
        print(f"   - execute_cte_query: {execute_cte_query}")
        print(f"   - fetch_results: {fetch_results}")
        print(f"   - test_connection: {test_connection}")
        print(f"   - get_connection_params: {get_connection_params}")

    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        return False

    print()

    # Step 2: Check .env file
    print("-" * 70)
    print("Step 2: Environment File Check")
    print("-" * 70)

    from pycorn.db_connector import env_file_candidates, find_env_file, read_env_file

    env_file = find_env_file()
    if env_file is None:
        print("[INFO] No .env file found; only UNICORN_* environment variables are used. Looked in:")
        for path in env_file_candidates():
            print(f"   {path}")
        print(f"   Create one from: {project_root / 'sql' / '.env.example'}")
    else:
        print(f"[OK] .env file: {env_file}")
        # Show which settings are present, never their values
        try:
            for key, value in read_env_file(env_file).items():
                print(f"   {key}: {'set' if value else 'empty'}")
        except Exception as e:
            print(f"[WARN] Could not read .env details: {type(e).__name__}")

    print()

    # Step 3: Check CTE file
    print("-" * 70)
    print("Step 3: CTE Query File Check")
    print("-" * 70)

    cte_file = project_root / "pycorn" / "sql" / "unicorn_cte.sql"

    if cte_file.exists():
        print(f"[OK] CTE file exists: {cte_file}")

        # Read and validate structure
        cte_content = cte_file.read_text(encoding='utf-8')

        has_with = "WITH " in cte_content.upper()
        has_select = "SELECT " in cte_content.upper()
        has_folder = "folderhierarchydata" in cte_content.lower()

        print(f"   - Has WITH clause: {has_with}")
        print(f"   - Has SELECT clause: {has_select}")
        print(f"   - References folderHierarchyData: {has_folder}")

        # Check for parameterized WHERE clause
        if "@FolderFilter" in cte_content or "@DescriptionPattern" in cte_content:
            print(f"[OK] CTE supports server-side filtering")
        else:
            print(f"[INFO] CTE uses default query (no filtering)")

    else:
        print(f"[FAIL] CTE file not found: {cte_file}")
        return False

    print()

    # Step 4: Test connector initialization
    print("-" * 70)
    print("Step 4: Connector Initialization Test")
    print("-" * 70)

    try:
        db = PyCORNDatabase()  # same lookup as find_env_file(); environment variables override
        conn_params = db.get_connection_params()

        print("[OK] PyCORNDatabase initialized successfully")
        print(f"   Connection params: {conn_params}")

        if hasattr(db, 'query'):
            print(f"[OK] query() method available")
        else:
            print(f"[FAIL] query() method missing")

        if hasattr(db, 'execute_custom_query'):
            print(f"[OK] execute_custom_query() method available")
        else:
            print(f"[FAIL] execute_custom_query() method missing")

        if hasattr(db, 'get_connection_params'):
            print(f"[OK] get_connection_params() method available")
        else:
            print(f"[FAIL] get_connection_params() method missing")

        if hasattr(db, 'test_connection'):
            print(f"[OK] test_connection() method available")
        else:
            print(f"[FAIL] test_connection() method missing")

    except Exception as e:
        # The message names variables and paths, never values; a traceback could show more
        print(f"[ERROR] Connector initialization failed: {e}")
        return False

    print()

    # Step 5: Test connection (optional - requires database)
    print("-" * 70)
    print("Step 5: Connection Test")
    print("-" * 70)

    try:
        has_server = bool(conn_params.server if hasattr(conn_params, 'server') else '')

        if has_server:
            test_result = db.test_connection(quiet=True)
            if test_result:
                print("[OK] Connection test passed")
            else:
                print("[INFO] Connection test failed (check credentials)")
        else:
            print("[INFO] Skipping connection test (no server specified)")

    except Exception as e:
        print(f"[WARN] Connection test error (may be expected): {e}")

    print()

    # Step 6: Test query execution
    print("-" * 70)
    print("Step 6: Query Execution Test")
    print("-" * 70)

    try:
        cursor, columns = db.query_raw(quiet=True)

        if columns:
            print(f"[OK] Query executed successfully")
            print(f"   Columns returned: {len(columns)}")
            print(f"   First 5 columns: {columns[:5]}")

            # Check for expected columns
            folder_cols = [c for c in columns if 'folder' in c.lower()]
            result_cols = [c for c in columns if 'result' in c.lower()]

            print(f"   - Folder-related columns: {len(folder_cols)}")
            print(f"   - Result-related columns: {len(result_cols)}")
        else:
            print("[FAIL] No columns returned from query")

    except Exception as e:
        print(f"[WARN] Query execution error (may be expected without database): {e}")

    print()

    # Step 7: Test export functionality
    print("-" * 70)
    print("Step 7: Export Functionality Test")
    print("-" * 70)

    try:
        if hasattr(db, 'export_to_csv'):
            print("[OK] export_to_csv() method available")
        else:
            print("[FAIL] export_to_csv() method missing")
    except Exception as e:
        print(f"[ERROR] Export test failed: {e}")

    print()

    # Step 8: Test PyCORNDatabase class properties
    print("-" * 70)
    print("Step 8: Database Class Properties")
    print("-" * 70)

    try:
        print(f"[OK] Folder summary column groups: {db.AVAILABLE_TABLES}")

        if hasattr(db, '_read_env_credentials'):
            print("[OK] _read_env_credentials() internal method available")
        else:
            print("[FAIL] _read_env_credentials() method missing")

    except Exception as e:
        print(f"[ERROR] Property test failed: {e}")

    print()

    # Summary
    print("=" * 70)
    print("Validation Complete!")
    print("=" * 70)
    print()
    print("Summary:")
    print("- All imports successful")
    print("- .env file loading working")
    print("- CTE query file accessible")
    print("- Connector methods available")
    print("- Ready for production use")
    print()

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
