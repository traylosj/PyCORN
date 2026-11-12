# -*- coding: utf-8 -*-
"""
Example usage of PyCORN database connector with Pythonic interface.

Shows how to interact with database tables as Python objects.
Security-first: uses environment variables, never hardcodes credentials, and never
prints connection values (server, port, database, user, password): only set/empty.
"""

import sys
import tempfile
from pathlib import Path

# Add project root to path so the script runs from any directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pycorn.db_connector import PyCORNDatabase

ENV_FILE = Path(__file__).parent / ".env"
ENV_TEMPLATE = Path(__file__).parent / ".env.example"


def main():
    """Demonstrate Pythonic database interaction."""

    print("=" * 60)
    print("PyCORN Database Connector - Example Usage")
    print("=" * 60)

    # =============================================================================
    # Example 1: Basic Connection Using Environment Variables
    # =============================================================================
    print("\n" + "-" * 60)
    print("Example 1: Connect using .env file")
    print("-" * 60)

    try:
        # Initialize connector - reads credentials from sql/.env
        db = PyCORNDatabase()
    except ValueError as e:
        print(f"\n[Setup error] (expected if .env not configured): {e}")
        setup_example()
        return

    # Prints only which settings are set, never their values
    print(f"\nConnection configured for: {db.get_connection_params()}")

    if not db.test_connection():
        print("\nwarning: Connection test failed. Check your .env file settings.")
        setup_example()
        return

    # Execute default CTE query once; the examples below reuse it
    try:
        result = db.query()
    except Exception as e:
        print(f"warning: query failed: {e}")
        return

    print(f"\nResult type: {type(result).__name__}")
    print(f"DataFrame shape: {result.shape()}")

    # =============================================================================
    # Example 2: Using Object-Oriented Table Access
    # =============================================================================
    print("\n" + "-" * 60)
    print("Example 2: Accessing logical 'tables' as objects")
    print("-" * 60)

    try:
        all_columns = result.column_names
        print(f"\nTotal columns: {len(all_columns)}")
        shown = ', '.join(all_columns[:20]) + ('...' if len(all_columns) > 20 else '')
        print(f"Columns: {shown}")

        # Column groups of the folder summary: 'folder', 'results', 'methods'
        for group, cols in result.tables.items():
            print(f"\n{group} columns ({len(cols)}): {', '.join(cols[:5])}{'...' if len(cols) > 5 else ''}")

        # Access as list of dictionaries (Pythonic iteration).
        # ascii(): UNICORN text can hold characters the Windows console (cp1252) can't print
        print("\nFirst row as dict:")
        if result.rows:
            for key, value in list(result.rows[0].items())[:5]:  # First 5 keys
                print(f"  {key}: {ascii(value)}")

    except Exception as e:
        print(f"warning: Example 2 error: {e}")

    # =============================================================================
    # Example 3: Executing Custom Queries
    # =============================================================================
    print("\n" + "-" * 60)
    print("Example 3: Execute custom SQL queries")
    print("-" * 60)

    try:
        # The 10 most recently created subfolders
        custom_sql = """
            SELECT TOP 10
                FolderID,
                ParentFolderID,
                Description AS FolderName,
                IsGlobal,
                Created,
                LastModified
            FROM dbo.Folder
            WHERE ParentFolderID IS NOT NULL
            ORDER BY Created DESC
        """

        custom_result = db.execute_custom_query(custom_sql)
        print(f"\nCustom query returned {custom_result.shape()}")
        print(ascii(custom_result.head().to_string()))

    except Exception as e:
        print(f"warning: Example 3 error: {e}")

    # =============================================================================
    # Example 4: Export Results to CSV
    # =============================================================================
    print("\n" + "-" * 60)
    print("Example 4: Export query results to CSV")
    print("-" * 60)

    try:
        # Data extracted from the database must not land in the repository:
        # write to the system temp folder
        export_path = Path(tempfile.gettempdir()) / "pycorn_example" / "unicorn_query_results.csv"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        result.df.to_csv(export_path, index=False)
        print(f"\nFile saved: {export_path}")

    except Exception as e:
        print(f"warning: Example 4 error: {e}")

    # =============================================================================
    # Example 5: Using raw cursor for advanced operations
    # =============================================================================
    print("\n" + "-" * 60)
    print("Example 5: Advanced usage with raw cursor access")
    print("-" * 60)

    try:
        # Get raw cursor and columns, then stream rows in batches
        cursor, columns = db.query_raw()
        total = 0
        try:
            while True:
                batch = cursor.fetchmany(500)
                if not batch:
                    break
                total += len(batch)
        finally:
            cursor.connection.close()
        print(f"\nStreamed {total} rows in batches of 500 ({len(columns)} columns)")

    except Exception as e:
        print(f"warning: Example 5 error: {e}")


def setup_example():
    """Show how to set up sql/.env, reporting only which settings are set (never values)."""

    print("\n" + "=" * 60)
    print("Setting Up PyCORN Database Connector")
    print("=" * 60)

    if not ENV_FILE.exists():
        print("\n[FAIL] .env file not found!")
        print("   Create it from the template:")
        print(f"   cd {Path(__file__).parent}")
        print("   copy .env.example .env    (Linux/macOS: cp .env.example .env)")
        if ENV_TEMPLATE.exists():
            print("\n   Then fill in these settings:")
            for key in _env_settings(ENV_TEMPLATE):
                print(f"   {key}=")
        return

    print(f"\n[OK] .env file exists at: {ENV_FILE.absolute()}")
    try:
        for key, value in _env_settings(ENV_FILE).items():
            print(f"   {key}: {'set' if value else 'empty'}")
    except Exception as e:
        print(f"   Error reading .env: {type(e).__name__}")


def _env_settings(path):
    """UNICORN_* keys of a .env file and their values (for set/empty checks only)."""
    settings = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, _, value = line.partition('=')
        key = key.strip().upper().replace('-', '_')
        if key.startswith('UNICORN_'):
            settings[key] = value.split(' #')[0].strip()
    return settings


if __name__ == '__main__':
    main()
