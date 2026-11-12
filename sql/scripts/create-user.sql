-- Create Login
USE master;
IF NOT EXISTS (SELECT 1 FROM master.sys.server_principals WHERE [name] = N'unicorn_read' AND [type] IN ('C','E', 'G', 'K', 'S', 'U'))
BEGIN
	CREATE LOGIN unicorn_read WITH PASSWORD = 'YOUR_PASSWORD_HERE', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
END

-- Below codes could be refactored into a loop iterating over a list of UNICORN databases on the server

-- Execute statement for each UNICORN database on the server.  The default UNICORN database name is `UNICORN`;
-- replace YOUR_UNICORN_DATABASE_1 with the name of yours
USE YOUR_UNICORN_DATABASE_1;
IF NOT EXISTS (SELECT principal_id FROM sys.database_principals WHERE name = 'db_read')
BEGIN
	CREATE USER db_read FOR LOGIN unicorn_read;
END
GRANT SELECT TO db_read;

-- Repeat for each further UNICORN database on the server (replace YOUR_UNICORN_DATABASE_2, or delete
-- this block if there is only one)
USE YOUR_UNICORN_DATABASE_2;
IF NOT EXISTS (SELECT principal_id FROM sys.database_principals WHERE name = 'db_read')
BEGIN
	CREATE USER db_read FOR LOGIN unicorn_read;
END
GRANT SELECT TO db_read;
