#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PyCORN Example Usage - Simplified (pymssql + .env + file CTE)

This example demonstrates the simplified PyCORN database connector.

Usage:
    python sql/example_usage.py

Or use directly in your Python scripts.
"""

import sys
from pathlib import Path


def main():
    """Show how to connect using pymssql with .env and file-based CTE."""

    print("=" * 70)
    print("PyCORN - PyMssql Connection Example")
    print("Connects to UNICORN database WITHOUT ODBC drivers")
    print("=" * 70)
    print()

    # Add project root to Python path for imports
    project_root = Path(__file__).parent.parent.resolve()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    print("[INFO] Added pycorn directory to Python path")
    print(f"[INFO] Project root: {project_root}")
    print()

    from pycorn.db_connector import DEFAULT_CTE_FILE as cte_file, find_env_file

    # Check for .env file (the same lookup PyCORNDatabase() uses)
    env_file = find_env_file()
    if env_file is None:
        print("[INFO] No .env file found; using UNICORN_* environment variables")
        print("Create one from: sql/.env.example")
    else:
        print(f"[OK] Using settings from: {env_file}")

    if not cte_file.exists():
        print(f"[INFO] CTE file expected at: {cte_file}")
    else:
        print(f"[OK] CTE query file exists: {cte_file}")

    print()
    print("-" * 70)
    print()

    # Try to import and test if possible
    try:
        from pycorn.db_connector import (
            PyCORNDatabase,
            execute_cte_query,
            fetch_results,
            get_connection_params,
            test_connection,
            get_cte_query,
            load_env,
        )

        print("[OK] All imports successful")
        print()

        # Check available database functions
        print("-" * 70)
        print("Available Connector Functions:")
        print(f"  - PyCORNDatabase class")
        print(f"  - execute_cte_query()")
        print(f"  - fetch_results()")
        print(f"  - get_connection_params()")
        print(f"  - test_connection()")
        print("-" * 70)
        print()

        # Settings: the .env file, overridden by UNICORN_* environment variables
        try:
            env = load_env()
        except ValueError as e:
            env = {}
            print(f"[INFO] {e}")
        server = env.get('UNICORN_SERVER', '')

        if server:
            print("[INFO] UNICORN_SERVER is set")

            # Test connection if credentials available
            conn_params = get_connection_params(
                server=server,
                database=env.get('UNICORN_DATABASE', ''),
                username=env.get('UNICORN_USER'),
                password=env.get('UNICORN_PASSWORD'),
                login_timeout=10,
                port=env.get('UNICORN_PORT') or None,
            )
            print(f"[INFO] Connection parameters configured")

            # Try to test connection (will fail if no DB, but shows API works)
            try:
                if test_connection(conn_params):
                    print("[OK] Connection test passed!")

                    # Execute CTE query; execute_cte_query() declares its filter variables
                    import pymssql
                    conn = pymssql.connect(**conn_params)
                    cursor = conn.cursor()

                    cursor, columns = execute_cte_query(cursor)
                    print(f"[OK] Query executed, returned {len(columns)} columns")

                    # Fetch first 5 rows
                    df = fetch_results(cursor, columns)
                    print(f"\nSample results ({min(5, len(df))} rows):")
                    print(df.head())

                    conn.close()
                else:
                    print("[INFO] Connection test skipped (check .env file)")
            except Exception as e:
                print(f"[FAIL] Query execution failed: {e}")
        else:
            print("[INFO] No server specified")
            print("Create sql/.env from sql/.env.example, or set UNICORN_SERVER and the other variables")

    except ImportError as e:
        print(f"[ERROR] Import failed: {e}")
        print("[INFO] Make sure pymssql and pandas are installed:")
        print("       pip install pymssql pandas")
    except FileNotFoundError as e:
        print(f"[FAIL] File not found: {e}")
        print("[WARN] Check that sql/.env exists with your credentials")

    print()
    print("-" * 70)
    print("=" * 70)


if __name__ == '__main__':
    main()
