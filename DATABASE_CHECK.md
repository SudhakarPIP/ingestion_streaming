# Database Check Instructions

## Issue
The application is configured to use database: **`eventdb`**
But you're checking database: **`pip`**

## Solution

### Option 1: Check the Correct Database (eventdb)

Connect to MySQL and check the `eventdb` database:

```sql
-- Connect to MySQL
mysql -u root -p

-- Use the correct database
USE eventdb;

-- Check event_inbox table
SELECT * FROM event_inbox 
WHERE tenant_id = 'tenant112' AND event_id = 'event112';

-- Check event_outbox table
SELECT * FROM event_outbox 
WHERE tenant_id = 'tenant112' AND event_id = 'event112';

-- List all tables
SHOW TABLES;

-- Check table structure
DESCRIBE event_inbox;
DESCRIBE event_outbox;
```

### Option 2: Change Database Configuration

If you want to use the `pip` database instead, update `application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/pip?createDatabaseIfNotExist=true&useSSL=false&serverTimezone=UTC
```

**Note**: After changing the database, you'll need to:
1. Restart the application
2. Let Flyway create the tables in the new database
3. Tables will be created automatically on startup

### Option 3: Verify Current Database Connection

Check what database the application is actually using:

```sql
-- Connect and check current database
mysql -u root -p

-- List all databases
SHOW DATABASES;

-- Check if eventdb exists
SHOW DATABASES LIKE 'eventdb';

-- Check if pip exists
SHOW DATABASES LIKE 'pip';
```

## Quick Verification

Run this SQL to check if records exist in eventdb:

```sql
USE eventdb;
SELECT COUNT(*) FROM event_inbox;
SELECT COUNT(*) FROM event_outbox;
SELECT * FROM event_inbox ORDER BY created_at DESC LIMIT 10;
SELECT * FROM event_outbox ORDER BY created_at DESC LIMIT 10;
```

