#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PyCORN Example Usage - Simplified (pymssql + .env + file CTE)

This example demonstrates the simplified PyCORN database connector.

Usage:
    python sql/example_usage.py

Or use directly in your Python scripts.
"""

import os
from pathlib import Path


def main():
    """Show how to connect using pymssql with .env and file-based CTE."""

    print("=" * 70)
    print("PyCORN - PyMssql Connection Example")
    print("Connects to UNICORN database WITHOUT ODBC drivers")
    print("=" * 70)
    print()

    # Check for .env file
    script_dir = Path(__file__).parent.resolve()
    env_file = script_dir / ".env"

    if not env_file.exists():
        print(f"[INFO] No .env file found at: {env_file}")
        print("Create one from: sql/.env.example")
    else:
        print(f"[INFO] Using credentials from: {env_file}")

    print()
    print("-" * 70)
    print()

    # Example code showing the connector usage
    example_code = '''
# Method 1: Using environment variables (recommended)
import os
from pycorn.db_connector import execute_cte_query, fetch_results
import pymssql

# Connect using .env file credentials
conn_params = {
    'server': os.getenv('UNICORN_SERVER', ''),
    'database': os.getenv('UNICORN_DATABASE', 'UNICORN'),
    'user': os.getenv('UNICORN_USER', None),
    'password': os.getenv('UNICORN_PASSWORD', '')  # Empty for Windows auth
}

if conn_params['server']:  # Only connect if server is specified
    connection = pymssql.connect(**{k: v for k, v in conn_params.items() if v})
    cursor = connection.cursor()

    # Execute CTE query - reads unicorn_cte.sql automatically!
    cursor, columns = execute_cte_query(cursor)

    # Get results as DataFrame
    df = fetch_results(cursor, columns)

    print(df.head())
    connection.close()


# Method 2: Pass explicit parameters (use environment variables or .env file)
from pycorn.db_connector import get_connection_params, test_connection

conn_params = get_connection_params(
    server=os.getenv('UNICORN_SERVER', 'YOUR_SERVER'),
    database=os.getenv('UNICORN_DATABASE', 'YOUR_DATABASE'),
    username=os.getenv('UNICORN_USER', 'sa'),
    password=os.getenv('UNICORN_PASSWORD', ''),  # Set in .env or environment
    login_timeout=15
)

if test_connection(conn_params):
    connection = pymssql.connect(**conn_params)
    cursor, columns = execute_cte_query(connection.cursor())
    df = fetch_results(cursor, columns)
'''

    print(example_code)

    print("-" * 70)
    print()

    print("=" * 70)
    print("Key Features:")
    print("=" * 70)
    print()
    print("1. Uses pymssql (no ODBC required)")
    print("2. Reads credentials from .env file")
    print("3. CTE queries loaded from files (sql/unicorn_cte.sql)")
    print("4. Works on Windows, Linux, macOS")
    print("-" * 70)
    print()


if __name__ == '__main__':
    main()
