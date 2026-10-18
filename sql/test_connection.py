#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Quick Connection Test for UNICORN Database

Usage:
    python sql/test_connection.py

This script uses the db_connector module to test database connection.
"""

import sys
import os
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

        # Load environment variables from sql/.env
        env_file = script_dir / "sql" / ".env"
        if env_file.exists():
            # Read .env file and set environment variables
            for line in env_file.read_text().strip().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()
            print(f"[INFO] Loaded credentials from: {env_file}")

        # Import after setting environment variables
        from pycorn.db_connector import get_connection_params, test_connection, execute_cte_query
        import pymssql

        print("[INFO] Connecting to database...")

        # Get connection parameters (uses .env if available, otherwise prompts)
        server = os.getenv('UNICORN_SERVER', '')
        database = os.getenv('UNICORN_DATABASE', 'UNICORN')
        user = os.getenv('UNICORN_USER')
        password = os.getenv('UNICORN_PASSWORD')

        if server:
            conn_params = get_connection_params(
                server=server,
                database=database,
                username=user if user else None,
                password=password if password else None,
                login_timeout=10  # Short timeout for testing
            )

            # Test connection first
            if not test_connection(conn_params):
                print("[ERROR] Connection test failed. Check credentials in sql/.env")
                return False
        else:
            print("[WARN] UNICORN_SERVER not found in environment variables")
            print("Create sql/.env file with your credentials or set UNICORN_SERVER env var")
            return False

        # Now connect and execute CTE query
        connection = pymssql.connect(**conn_params)
        cursor = connection.cursor()

        try:
            # Execute CTE query (reads unicorn_cte.sql by default)
            cursor, columns = execute_cte_query(cursor)
            print("[OK] Connection successful!")
            print(f"[INFO] Query returned {len(columns)} columns")

            # Fetch sample results
            rows = cursor.fetchmany(3)
            if rows:
                for row in rows:
                    print(f"   - {row}")

        except Exception as e:
            print(f"[ERROR] Query failed: {e}")
            return False

    except ImportError as e:
        print(f"[ERROR] Module import failed: {e}")
        print("[INFO] Make sure pymssql and pandas are installed")
        print("       pip install pymssql pandas")
        return False

    except FileNotFoundError as e:
        print(f"[ERROR] File not found: {e}")
        print("[WARN] Check that sql/.env exists with your credentials")
        print("       And unicorn_cte.sql is available at the default path")
        return False

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {str(e)[:100]}")
        if "Login failed" in str(e) or "untrusted domain" in str(e).lower():
            print("[TIPS] This requires SQL Server Authentication:")
            print("  1. Enable Mixed Mode on SQL Server, OR")
            print("  2. Create a dedicated SQL user with password")
        return False

    finally:
        try:
            connection.close()
        except:
            pass

    print()
    print("=" * 70)
    print("Test completed successfully!")
    print("=" * 70)

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
