-- Removes read-only database users if no longer required
-- Removal must be performed for each UNICORN database on the server
-- The default UNICORN database name is `UNICORN`; replace the YOUR_UNICORN_DATABASE_* placeholders
-- with the names of yours (delete the second block if there is only one)

USE YOUR_UNICORN_DATABASE_1;
DROP USER db_read;

USE YOUR_UNICORN_DATABASE_2;
DROP USER db_read;

USE master;
DROP LOGIN unicorn_read;
