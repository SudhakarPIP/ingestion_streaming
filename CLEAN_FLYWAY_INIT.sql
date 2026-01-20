-- Clean Flyway Initialization Script
-- Run this SQL script to completely clean the pip database and let Flyway start fresh

-- Connect to MySQL first: mysql -u root -p

-- Use pip database
USE pip;

-- Drop all application tables
DROP TABLE IF EXISTS event_outbox;
DROP TABLE IF EXISTS event_inbox;

-- Drop Flyway schema history table
DROP TABLE IF EXISTS flyway_schema_history;

-- Verify everything is clean
SHOW TABLES;

-- Should return: Empty set (0.00 sec)

-- After running this script, restart your Spring Boot application
-- Flyway will automatically:
-- 1. Create flyway_schema_history table
-- 2. Run V1__Create_event_inbox_table.sql
-- 3. Run V2__Create_event_outbox_table.sql

