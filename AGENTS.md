# PyCORN Agent Collaboration Notes

## Our Working Patterns

### 🔐 Security-First Approach - CRITICAL RULES

**NEVER write credentials in ANY form:**
- **❌ NEVER write passwords, usernames, or secrets to chat history/messages**
- **❌ NEVER write credentials to any file (.env, config, examples, documentation)**
- **❌ NEVER commit real credential values to git repositories**
- **❌ NEVER log or print actual password values during testing/debugging**
- **✅ ONLY use environment variable names (e.g., `os.getenv('UNICORN_PASSWORD')`)**
- **✅ ALWAYS use `.env.example` template with empty placeholders**
- **✅ NEVER include real server IPs, database names with passwords in examples**

This rule applies to:
- All agent conversations and chat history
- Example code blocks and documentation
- Test files and sample data
- README files and user guides
- Commit messages and pull requests

**Why this matters:** Chat history and example files can be exported, shared, or leaked. Even appearing to show "example credentials" is dangerous - use ONLY placeholder values like `YOUR_PASSWORD_HERE` or empty strings.

### 🔐 Code Implementation Rules
- Use `os.getenv('UNICORN_SERVER', '')` instead of hardcoded server addresses
- Use `os.getenv('UNICORN_DATABASE', 'YOUR_DATABASE')` instead of hardcoded database names
- Use `os.getenv('UNICORN_PASSWORD', '')` instead of any password string
- Use `.env.example` with empty placeholders, never real values

### 🐍 Pure Python, No ODBC
- Use `pymssql` for TCP/IP connections to SQL Server
- Install with: `pip install pymssql pandas`
- Works on Windows, Linux, macOS without ODBC drivers
- Connector reads CTE queries from files, not embedded strings

### 📁 File-Based CTE Queries
- Default CTE file location: `sql/unicorn_cte.sql` (relative to project root)
- Connector (`db_connector.py`) reads from this file by default
- Easy to update queries by editing SQL file directly
- Custom queries can be passed via `-c/--cte-file` CLI option

### 🧪 Testing Strategy
- Test connection separately: `python sql/test_connection.py`
- Validate CTE file exists: `python sql/test_cte_file_reading.py`
- Use simplified test files - no ODBC code mixed in
- All test scripts read credentials from `.env` file

### 🧹 Code Cleanup Principles
- Keep only pymssql connection methods
- Remove pyodbc/ODBC references entirely
- Simplify examples to show single approach
- Consolidate documentation into clear guides

### 📋 Key Files & Their Purposes

| File | Purpose |
|------|---------|
| `sql/.env` | Your credentials (gitignored for security) |
| `sql/.env.example` | Template others can copy to create `.env` |
| `sql/scripts/unicorn_cte.sql` | CTE query file connector reads by default |
| `pycorn/db_connector.py` | Main database connector module |
| `sql/test_connection.py` | Test connection using .env |
| `sql/test_cte_file_reading.py` | Validate CTE file loading |
| `sql/example_usage.py` | Usage examples (pymssql only) |

### 🛡️ GitIgnore Rules We Use

```gitignore
.env                    # All .env files anywhere
sql/.env                # Specific rule for sql/ folder
.venv/                  # Python virtual environment
*.pem, *.key            # Private keys
secrets.txt             # secrets module output
.env.example           # Optional: ignore template too (we track it)
```

### 🚀 Quick Start Workflow

1. Copy template: `copy sql/.env.example .env`
2. Edit with your credentials in `sql/.env`
3. Run test: `python sql/test_connection.py`
4. Use connector: `from pycorn.db_connector import execute_cte_query`

### 🔄 Version Control Best Practices

- Never commit `.env` files to git (gitignored)
- Commit `.env.example` as template for team sharing
- Track SQL query files in `sql/` folder for version control
- Keep documentation separate from implementation

---

## Connection Flow

The `db_connector.py` module provides these key functions:

1. **get_cte_query(filepath)** - Read CTE from SQL file (defaults to `sql/unicorn_cte.sql`)
2. **get_connection_params(...)** - Build connection parameters from env vars or explicit args
3. **test_connection(conn_params)** - Test if connection works before using credentials
4. **execute_cte_query(cursor, filepath)** - Execute CTE query and return cursor + columns
5. **fetch_results(cursor, columns)** - Convert cursor results to pandas DataFrame

### Connection Options

- **SQL Authentication**: Provide `server`, `database`, `user`, `password` parameters
- **Windows Authentication**: Use Windows credentials via `host_name=server` parameter (requires LOCALSERTYPE setting)

**Note**: For TCP/IP connections, SQL Server authentication is recommended over Windows auth since pymssql doesn't support Kerberos/NTLM over pure TCP/IP by default.

---

Generated during PyCORN setup with Claude Code
