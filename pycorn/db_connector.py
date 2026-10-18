# -*- coding: utf-8 -*-
"""
PyCORN Database Connector (SQL Server - No ODBC)
Connects to UNICORN 7.11 SQL Server database using pure Python TCP/IP (pymssql).

(c)2026 - traylosj
Based on unicorn_cte.sql from the project's sql/scripts folder

This version reads CTE queries from SQL files instead of embedding them.
"""

import argparse
from pathlib import Path
from typing import Optional, Tuple, Any


def get_cte_query(filepath: Optional[str] = None) -> str:
    """
    Read the CTE query from a SQL file.

    Parameters
    ----------
    filepath : str, optional
        Path to unicorn_cte.sql file (defaults to 'sql/scripts/unicorn_cte.sql')

    Returns
    -------
    str
        The CTE SQL query string

    Raises
    ------
    FileNotFoundError
        If the specified file doesn't exist
    """
    default_path = "sql/scripts/unicorn_cte.sql"

    if filepath:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"CTE SQL file not found: {filepath}")
        return path.read_text(encoding='utf-8').strip()
    else:
        default_path_obj = Path(default_path)
        if default_path_obj.exists():
            return default_path_obj.read_text(encoding='utf-8').strip()
        raise FileNotFoundError(
            f"Default CTE SQL file not found at: {default_path}. "
            "Please place unicorn_cte.sql in the 'sql/scripts/' directory."
        )


try:
    import pymssql
except ImportError:
    print("Installing pymssql...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'pymssql'])
    import pymssql


def get_connection_params(server: str, database: str,
                          username: Optional[str] = None,
                          password: Optional[str] = None,
                          login_timeout: int = 30) -> dict:
    """
    Generate connection parameters for SQL Server using pymssql.

    Parameters
    ----------
    server : str
        Database server name or IP address (e.g., 'SERVERNAME' or '192.168.1.10')
    database : str
        Database name (e.g., 'UNICORN', 'UNICO711')
    username : str, optional
        Username for SQL Server authentication (None for Windows auth)
    password : str, optional
        Password for SQL Server authentication (omit for Windows auth)
    login_timeout : int, optional
        Timeout in seconds for connection attempts

    Returns
    -------
    dict
        Connection parameters as keyword arguments for pymssql.connect()
    """

    conn_params = {
        'server': server,
        'database': database,
        'login_timeout': login_timeout,
    }

    if username and password:
        conn_params['user'] = username
        conn_params['password'] = password
    else:
        # Windows Authentication (Integrated Security)
        conn_params['host_name'] = server

    return conn_params


def test_connection(conn_params: dict) -> bool:
    """
    Test database connection.

    Parameters
    ----------
    conn_params : dict
        Connection parameters from get_connection_params()

    Returns
    -------
    bool
        True if connection successful, False otherwise
    """
    try:
        connection = pymssql.connect(**conn_params)
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


def execute_cte_query(cursor, filepath: Optional[str] = None) -> Tuple[Any, list]:
    """
    Execute the CTE query from a SQL file.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Database cursor
    filepath : str, optional
        Path to custom CTE SQL file. If None, reads from sql/unicorn_cte.sql

    Returns
    -------
    tuple
        (cursor, columns_list) where cursor has results

    Raises
    ------
    FileNotFoundError
        If the CTE SQL file is not found
    """
    # Read CTE query from file
    cte_sql = get_cte_query(filepath)

    try:
        cursor.execute(cte_sql)
        columns = [desc[0] for desc in cursor.description]
        return cursor, columns
    except Exception as e:
        raise RuntimeError(f"Database query execution failed: {e}")


def fetch_results(cursor, columns: list) -> Any:
    """
    Fetch results from cursor and convert to DataFrame.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Cursor with executed query
    columns : list
        Column names from cursor description

    Returns
    -------
    pandas.DataFrame
        Query results as DataFrame
    """
    import pandas as pd

    # Fetch all data and convert to dictionary
    rows = cursor.fetchall()

    # Create dictionary of columns to values
    data_dict = {col: [] for col in columns}

    for row in rows:
        for i, col in enumerate(columns):
            data_dict[col].append(row[i])

    return pd.DataFrame(data_dict)


def export_to_csv(df: Any, output_path: str) -> None:
    """
    Export query results to CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        Query results
    output_path : str
        Path to output CSV file
    """
    df.to_csv(output_path, index=False)
    print(f"Results exported to: {output_path}")


def create_database_user(sql_file: str, server: str, database: str,
                         username: str, password: str) -> None:
    """
    Create a read-only user for the UNICORN database using SQL Server authentication.

    Parameters
    ----------
    sql_file : str
        Path to create-user.sql template
    server : str
        Database server name
    database : str
        Database name
    username : str
        New username to create
    password : str
        Password for new user
    """
    import pymssql

    conn = pymssql.connect(server=server, database=database,
                           user=username, password=password)
    cursor = conn.cursor()

    with open(sql_file, 'r') as f:
        create_sql = f.read()

    # Replace placeholders if needed
    try:
        cursor.execute(create_sql.replace('{USERNAME}', username)
                      .replace('{PASSWORD}', password))
        print(f"User '{username}' created successfully")
    except Exception as e:
        print(f"Error creating user: {e}")

    cursor.close()
    conn.close()


def main():
    """
    Main function with command-line interface.

    Usage examples:

    # SQL Server Authentication (TCP/IP)
    python db_connector.py -s "SERVERNAME" -d "UNICORN" \
        -u "username" -p "password" -o "output.csv"

    # Custom CTE file
    python db_connector.py -s "SERVERNAME" -d "UNICORN" \
        -u "username" -p "password" -c "custom_query.sql" -o "output.csv"
    """
    parser = argparse.ArgumentParser(
        description='Connect to UNICORN 7.11 database using pymssql (no ODBC)'
    )

    parser.add_argument('-s', '--server', required=True,
                        help='Database server (IP address or hostname)')
    parser.add_argument('-d', '--database', required=True,
                        help='Database name (e.g., UNICORN, UNICO711)')
    parser.add_argument('--windows-auth', action='store_true',
                        help='Use Windows Authentication (requires LOCALSERTYPE=SNAPSHOT)')
    parser.add_argument('-u', '--user', help='Username for SQL Server authentication')
    parser.add_argument('-p', '--password', help='Password for SQL Server authentication')
    parser.add_argument('-c', '--cte-file', dest='cte_file', default=None,
                        help='Custom CTE SQL file path (optional, uses sql/unicorn_cte.sql by default)')
    parser.add_argument('-o', '--output', default='unicorn_data.csv',
                        help='Output CSV file path (default: unicorn_data.csv)')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Suppress connection/test output')

    args = parser.parse_args()

    # Build connection parameters
    conn_params = get_connection_params(
        server=args.server,
        database=args.database,
        username=args.user if args.user else None,
        password=args.password if args.password and not args.windows_auth else None,
        login_timeout=30
    )

    # For Windows auth via named pipes (requires LOCALSERTYPE)
    if args.windows_auth:
        print("Warning: pymssql doesn't support native Windows Authentication over TCP/IP")
        print("Use one of these alternatives:")
        print("  1. SQL Server authentication (-u -p flags)")
        print("  2. ODBC driver with integrated security")
        print("  3. Named pipes connection (requires LOCALSERTYPE=SNAPSHOT)")
        return

    # Test connection if credentials provided or no password
    if not args.quiet:
        print(f"Connecting to {args.server}:{args.database}...")

    try:
        if test_connection(conn_params):
            if not args.quiet:
                print("Connection successful!")
        else:
            return

        # Create cursor and execute query
        cursor = pymssql.connect(**conn_params)
        cursor = cursor.cursor()

        try:
            if not args.quiet:
                print("Executing CTE query...")

            cursor, columns = execute_cte_query(cursor, filepath=args.cte_file)

            if not args.quiet:
                print(f"Query executed successfully!")
                print(f"Columns returned: {len(columns)}")

            # Fetch and display results
            df = fetch_results(cursor, columns)

            if not args.quiet:
                print(f"\nResults shape: {df.shape}")
                print("\nFirst 5 rows:")
                print(df.head())

            # Export to CSV if output path specified
            if args.output:
                export_to_csv(df, args.output)

        finally:
            cursor.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
