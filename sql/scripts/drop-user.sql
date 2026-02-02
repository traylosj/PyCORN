-- Removes read-only database users if no longer required
-- Removal must be performed for each UNICORN database on the server

USE UNICORN_7_11;
DROP USER db_read;

USE UNICORN_7_3;
DROP USER db_read;

USE master;
DROP LOGIN unicorn_read;
