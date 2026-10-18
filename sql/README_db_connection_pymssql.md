# UNICORN 7.11 Database Connection (No ODBC Required)

This guide shows how to connect to the UNICORN 7.11 SQL Server database **without requiring ODBC drivers**. We use `pymssql`, a pure Python TCP/IP client.

## Why No ODBC?

- **ODBC requires** Microsoft driver installation
- **pymssql uses** standard TCP/IP sockets (like any web server)
- **Works on**: Windows, Linux, Mac - no special drivers needed
- **Good for**: CI/CD pipelines, containers, remote servers

## Installation

```bash
pip install pymssql pandas
```

Or via requirements file:
```bash
pip install -r requirements.txt
# Where requirements.txt contains:
pymssql>=2.1.0
pandas>=1.5.0
```

## Quick Start

### Interactive Python (File-Based CTE)

**Important**: The connector now reads CTE queries from `sql/unicorn_cte.sql` instead of embedding them in code.

```python
from pycorn.db_connector import get_connection_params, execute_cte_query, fetch_results
import pymssql
import pandas as pd

# 1. Connection parameters
conn_params = {
    'server': os.getenv('UNICORN_SERVER', ''),      # NEVER hardcode - use env var
    'database': os.getenv('UNICORN_DATABASE', ''),  # NEVER hardcode - use env var
    'user': os.getenv('UNICORN_USER', ''),          # Use env var or omit for Windows auth
    'password': os.getenv('UNICORN_PASSWORD')       # NEVER hardcode password
}

# 2. Connect
connection = pymssql.connect(**conn_params)
cursor = connection.cursor()

# 3. Execute CTE query (reads from sql/unicorn_cte.sql automatically)
cursor, columns = execute_cte_query(cursor)
# Or specify custom file: cursor, columns = execute_cte_query(cursor, filepath="path/to/custom_query.sql")

# 4. Fetch results as DataFrame
df = fetch_results(cursor, columns)

# 5. Display results
print(df.head())

connection.close()
```

### Command Line Interface

```bash
python pycorn\db_connector.py \
    -s "192.168.1.10" \
    -d "UNICORN" \
    -u "sa" \
    -p "your_password" \
    -o "results.csv"

# With custom CTE file:
python pycorn\db_connector.py \
    -s "SERVER" -d "UNICORN" -u "user" -p "pwd" \
    -c "path/to/custom_query.sql" \
    -o "output.csv"
```

## Connection Modes

### SQL Server Authentication (Recommended for TCP/IP)

Use when:
- Connecting over the internet
- Server uses Windows authentication disabled
- You prefer password over trust

```python
conn_params = get_connection_params(
    server="192.168.1.10",
    database="UNICORN",
    username="sa",
    password="your_secure_password",
    login_timeout=30  # seconds to wait for connection
)
```

### Windows Authentication (via TCP/IP - Requires Configuration)

Use when:
- Server uses only Windows authentication
- You're on a trusted network

Note: This requires the server to be configured to allow `WIN_AUTH`. By default, `pymssql` over TCP/IP will try SQL auth first. If that fails and you've set up proper permissions, it may work with just credentials omitted.

```python
conn_params = get_connection_params(
    server="MSSQLSERVER",  # Use instance name
    database="UNICORN",
    username=None,         # Omit for Windows auth
    password=None          # Omit for Windows auth
)
```

### Using Environment Variables (Best Practice)

Never hardcode passwords in source code:

```python
import os
from pycorn.db_connector import get_connection_params, execute_cte_query, fetch_results

conn_params = get_connection_params(
    server=os.getenv('UNICORN_SERVER', 'localhost'),
    database=os.getenv('UNICORN_DATABASE', 'UNICORN'),
    username=os.getenv('UNICORN_USER', 'sa'),
    password=os.getenv('UNICORN_PASSWORD')  # Set in environment
)

connection = pymssql.connect(**conn_params)
cursor = connection.cursor()
cursor, columns = execute_cte_query(cursor)
df = fetch_results(cursor, columns)
```

Set environment variables:

```bash
# Linux/Mac
export UNICORN_SERVER="192.168.1.10"
export UNICORN_DATABASE="UNICORN"
export UNICORN_USER="sa"
export UNICORN_SERVER="$YOUR_SERVER_FROM_ENV_FILE"
export UNICORN_DATABASE="$YOUR_DATABASE_NAME"
export UNICORN_USER="$YOUR_USERNAME"
export UNICORN_PASSWORD="$YOUR_SECURE_PASSWORD_FROM_VAULT"

# Windows CMD - Set from environment or secure source
set UNICORN_SERVER=%YOUR_SERVER_FROM_ENV%
set UNICORN_DATABASE=%YOUR_DATABASE_NAME%
set UNICORN_USER=%YOUR_USERNAME%
set UNICORN_PASSWORD=%YOUR_SECURE_PASSWORD%

# PowerShell - Load from secure vault or .env file
$env:UNICORN_SERVER="$env:YOUR_SERVER"
$env:UNICORN_DATABASE="$env:YOUR_DATABASE"  
$env:UNICORN_USER="$env:YOUR_USERNAME"
$env:UNICORN_PASSWORD="$env:YOUR_SECURE_PASSWORD_FROM_VAULT"

# CRITICAL: NEVER set real passwords in scripts or bash history!
# Use a secrets manager (HashiCorp Vault, AWS Secrets Manager, Azure Key Vault)
# Or store in .env file and load via environment variables only
```

## Advanced Usage

### Timeout Configuration

Control connection timeout (default: 30 seconds):

```python
conn_params = get_connection_params(
    server=os.getenv('UNICORN_SERVER', ''),        # Set via environment variable
    database=os.getenv('UNICORN_DATABASE', ''),    # Set via environment variable
    username=os.getenv('UNICORN_USER', ''),        # Use env var or omit for Windows auth
    password=os.getenv('UNICORN_PASSWORD'),        # NEVER hardcode - use env var
    login_timeout=60  # seconds to wait for connection
)
```

### Custom CTE Queries

The connector now supports custom CTE SQL files:

```python
from pycorn.db_connector import get_cte_query

# Read from file
custom_sql = get_cte_query(filepath="path/to/my_query.sql")

# Or use CLI:
# python db_connector.py -s "SERVER" -d "UNICORN" \
#     -c "path/to/custom_query.sql" -o "output.csv"
```

### Handling Large Datasets

For queries returning millions of rows:

```python
from pycorn.db_connector import get_connection_params, execute_cte_query
import pymssql

# CRITICAL: Use environment variables - NEVER hardcode credentials!
import os

conn_params = get_connection_params(
    server=os.getenv('UNICORN_SERVER', ''),
    database=os.getenv('UNICORN_DATABASE', ''),
    username=os.getenv('UNICORN_USER', ''),
    password=os.getenv('UNICORN_PASSWORD')  # Set from secure source (vault, env var)
)

connection = pymssql.connect(**conn_params)
cursor = connection.cursor()
cursor, columns = execute_cte_query(cursor)

# Fetch in chunks for memory efficiency
def fetch_chunks(cursor, chunk_size=10000):
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        yield rows

for row_batch in fetch_chunks(cursor):
    # Process batch here
    pass

connection.close()
```

## Troubleshooting

### "Unknown server name or incorrect port number"

**Solution**: Server address should be accessible via TCP/IP. Check firewall allows port 1433 (default SQL port).

```python
# Try with explicit port
conn_params['port'] = 1433
```

### "Login failed for user 'sa'"

**Causes**:
- Password incorrect
- User locked/disabled
- SQL authentication not enabled on server

**Solutions**:
1. Verify username/password with SQL Server Management Studio (SSMS)
2. Enable SQL Server Authentication in SQL Server Configuration Manager
3. Create a dedicated read-only user (see create-user.sql)

### "Connection timed out"

**Causes**:
- Firewall blocking port 1433
- Server overloaded
- Network issues

**Solutions**:
1. Check Windows Firewall/Network security allows SQL traffic
2. Increase timeout: `login_timeout=120`
3. Test connectivity: `telnet SERVER_NAME 1433`

### "Cannot open database 'UNICORN'"

**Cause**: Wrong database name

**Solution**: Verify with SSMS or query sys.databases first. UNICORN may be installed as:
- `UNICORN`
- `UNICO711`
- `UNICORNDATA`
- Custom name

### File-Based CTE Errors

If you get "CTE SQL file not found":

**Solution**: Ensure `sql/unicorn_cte.sql` exists in the same directory as `db_connector.py`, or specify a custom path:

```python
cursor, columns = execute_cte_query(cursor, filepath="/path/to/my_cte.sql")
```

## File Structure

```
PyCORN/
├── pycorn/
│   ├── db_connector.py          # Main connector with file-based CTE reading
│   └── __init__.py
└── sql/
    ├── unicorn_cte.sql          # Default CTE query (required)
    ├── create-user.sql          # Template for creating dedicated users
    └── drop-user.sql            # Script to remove test users
```

## Creating a Dedicated User (Recommended for Production)

See `sql/create-user.sql` template to create a read-only user:

```sql
-- TEMPLATE: Create a dedicated read-only user
-- IMPORTANT: Replace password with value from secure source (vault, secrets manager)
-- Never use default passwords or example passwords in production!

CREATE LOGIN [$YourDedicatedUser] WITH PASSWORD = $SECURE_PASSWORD_FROM_VAULT;
USE [UNICORN];
CREATE USER [$YourDedicatedUser] FOR LOGIN [$YourDedicatedUser];
ALTER ROLE db_datareader ADD MEMBER [$YourDedicatedUser];
-- GRANT SELECT ON SCHEMA::dbo TO [$YourDedicatedUser];  -- Optional: grant only needed permissions
```

Then use in Python:
```python
conn_params = get_connection_params(
    server="YOUR_SERVER",
    database="UNICORN",
    username="PycornReadOnly",
    password="StrongPassword123!"  # Set securely, not in code
)
```

## Testing Your Connection

Test before committing to your script:

```python
import pymssql

# CRITICAL SECURITY: Load credentials from environment variables only!
try:
    conn = pymssql.connect(
        server=os.getenv('UNICORN_SERVER', ''),
        database=os.getenv('UNICORN_DATABASE', ''),
        user=os.getenv('UNICORN_USER', '') if os.getenv('UNICORN_PASSWORD') is None else os.getenv('UNICORN_USER'),
        password=os.getenv('UNICORN_PASSWORD')  # Use env var or omit for Windows auth
    )
    print("✅ Connected successfully")
    cursor = conn.cursor()
    
    # Test that CTE file is readable
    cursor, columns = execute_cte_query(cursor)
    print(f"✅ CTE query loaded with {len(columns)} columns")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

## Security Best Practices

1. **Never hardcode passwords** in source control
   - Use environment variables or secret managers

2. **Use least privilege principle**
   - Create dedicated read-only users (see `create-user.sql`)
   - Don't use `sa` (sysadmin) in production

3. **Enable SSL encryption** (optional, recommended for production):
   
   ```python
   # Note: pymssql basic auth doesn't support encrypted connections easily
   # For encrypted connections, consider using pyodbc with SQL Server Native Client
   ```

4. **Rotate credentials regularly**
   - Update passwords every 90 days minimum

5. **Log connection attempts** (for audit trails):
   
   ```python
   import logging
   
   logging.basicConfig(filename='db_access.log', level=logging.INFO)
   logging.info(f"Database access: {server}")
   ```

## Migration from Embedded to File-Based CTE

**Before** (embedded SQL - harder to maintain):
```python
cte_sql = """
WITH folderHierarchyData as ...
SELECT * FROM folderSummary
"""
cursor.execute(cte_sql)
```

**After** (file-based - cleaner and maintainable):
```python
cursor, columns = execute_cte_query(cursor)  # Auto-reads sql/unicorn_cte.sql
# Or:
cursor, columns = execute_cte_query(cursor, filepath="custom_query.sql")
```

## Summary

- ✅ **No ODBC drivers required** - uses pure Python TCP/IP with pymssql
- ✅ **File-based CTE queries** - cleaner code, easier to maintain
- ✅ Works on Windows, Linux, macOS (same code everywhere)
- ✅ Good for CI/CD pipelines, containers, remote servers

### Key Changes from Previous Version

The connector now reads CTE queries from files instead of embedding them:

1. **`get_cte_query(filepath)`** - Reads SQL from file or defaults to `sql/unicorn_cte.sql`
2. **`execute_cte_query(cursor, filepath=None)`** - Automatically loads the query
3. **CLI `-c/--cte-file` option** - Specify custom CTE files via command line

This makes:
- Updates easier (edit SQL file, no code changes)
- Queries maintainable (version control works properly)
- Security better (don't accidentally commit sensitive queries)

## Summary

- **Use pymssql** for no-driver TCP/IP connections to SQL Server
- **Install**: `pip install pymssql pandas`
- **Connect**: Just need server address, not ODBC drivers
- **Works on**: Linux, macOS, Windows (same code everywhere)
- **See also**: 
  - `pycorn/db_connector.py` - Main connector script (now file-based CTE!)
  - `sql/README_db_connection_pymssql.md` - This guide
  - `sql/unicorn_cte.sql` - Default CTE query that gets loaded automatically

🤖 Generated with [Claude Code](https://claude.com/claude-code)
