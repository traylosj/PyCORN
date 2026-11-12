#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Quick Connection Test for UNICORN Database

Usage:
    python sql/test_connection.py

This script uses the db_connector module to test database connection. Settings come
from the same place PyCORNDatabase() reads them (see find_env_file()); UNICORN_*
environment variables override the file.
"""

import sys
from pathlib import Path


def main():
    """Test database connection using db_connector module."""

    print("=" * 70)
    print("UNICORN Database Connection Test")
    print("=" * 70)
    print()

    try:
        # Add project root to Python path
        script_dir = Path(__file__).parent.parent.resolve()
        if str(script_dir) not in sys.path:
            sys.path.insert(0, str(script_dir))

        print("[INFO] Using db_connector module from pycorn.db_connector")
        from pycorn.db_connector import PyCORNDatabase, find_env_file

        env_file = find_env_file()
        if env_file:
            print(f"[INFO] Settings file: {env_file}")
        else:
            print("[INFO] No .env file found; using UNICORN_* environment variables")

        db = PyCORNDatabase()
        print("[INFO] Connecting to database...")
        if not db.test_connection(quiet=True):
            print("[ERROR] Connection test failed. Check the settings in your .env")
            return False

        # Folder summary query (pycorn/sql/unicorn_cte.sql)
        result = db.query(quiet=True)
        print("[OK] Connection successful!")
        print(f"[INFO] Query returned {len(result.column_names)} columns, {len(result.df)} rows")

    except ImportError as e:
        print(f"[ERROR] Module import failed: {e}")
        print("[INFO] Make sure pymssql and pandas are installed")
        print("       pip install pymssql pandas")
        return False

    except FileNotFoundError as e:
        print(f"[ERROR] File not found: {e}")
        print("[WARN] Check that unicorn_cte.sql is available at the default path")
        return False

    except Exception as e:
        # Connector messages name variables, never values (ConnectionError is redacted)
        print(f"[ERROR] {type(e).__name__}: {str(e)[:300]}")
        if "Login failed" in str(e) or "untrusted domain" in str(e).lower():
            print("[TIPS] This requires SQL Server Authentication:")
            print("  1. Enable Mixed Mode on SQL Server, OR")
            print("  2. Create a dedicated SQL user with password")
        return False

    print()
    print("=" * 70)
    print("Test completed successfully!")
    print("=" * 70)

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
