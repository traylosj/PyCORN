-- Create Login
USE master;
IF NOT EXISTS (SELECT 1 FROM master.sys.server_principals WHERE [name] = N'unicorn_read' AND [type] IN ('C','E', 'G', 'K', 'S', 'U'))
BEGIN
	CREATE LOGIN unicorn_read WITH PASSWORD = 'PASSWORD', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
END

-- Below codes could be refactored into a loop iterating over a list of UNICORN databases on the server

-- Execute statement for each UNICORN database on the server.  Normally the database is called `UNICORN` but has been renamed here
USE UNICORN_7_11;
IF NOT EXISTS (SELECT principal_id FROM sys.database_principals WHERE name = 'db_read')
BEGIN
	CREATE USER db_read FOR LOGIN unicorn_read;
END
GRANT SELECT TO db_read;

-- Execute statement for each UNICORN database on the server.  Normally the database is called `UNICORN` but has been renamed here
USE UNICORN_7_3;
IF NOT EXISTS (SELECT principal_id FROM sys.database_principals WHERE name = 'db_read')
BEGIN
	CREATE USER db_read FOR LOGIN unicorn_read;
END
GRANT SELECT TO db_read;
