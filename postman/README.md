# Postman Collection for Event Ingestion API

This folder contains a Postman collection for testing all APIs in the Event Ingestion Service.

## Import Instructions

1. **Open Postman**
2. Click **Import** button (top left)
3. Select **File** tab
4. Choose `Event_Ingestion_API.postman_collection.json`
5. Click **Import**

## Environment Variables

The collection uses the following variables:

- `baseUrl`: Base URL of the service (default: `http://localhost:8080`)
- `correlationId`: Correlation ID for request tracing (auto-generated)

### Setting up Environment Variables

1. In Postman, click on **Environments** (left sidebar)
2. Create a new environment (or use the default)
3. Add variable:
   - **Variable**: `baseUrl`
   - **Initial Value**: `http://localhost:8080`
   - **Current Value**: `http://localhost:8080`

## Collection Structure

### 1. Event APIs
- **Ingest Single Event**: POST `/v1/events`
- **Ingest Bulk Events**: POST `/v1/events/bulk`
- **Get Event**: GET `/v1/events/{tenantId}/{eventId}`

### 2. Outbox APIs
- **Get Outbox Summary**: GET `/v1/outbox/summary`
- **Get Outbox Summary - Last 7 Days**: GET `/v1/outbox/summary` (no params)
- **Retry Failed Events**: POST `/v1/outbox/retry`

### 3. Health & Metrics APIs
- **Health Check**: GET `/actuator/health`
- **Metrics**: GET `/actuator/metrics`
- **Events Ingested Metric**: GET `/actuator/metrics/events_ingested_total`
- **Prometheus Metrics**: GET `/actuator/prometheus`

### 4. Negative Test Cases
- Various validation error scenarios

## Usage Examples

### 1. Ingest a Single Event

```json
POST http://localhost:8080/v1/events
Content-Type: application/json
X-Correlation-Id: req-123

{
  "tenantId": "tenant-123",
  "eventId": "event-456",
  "eventType": "user.action",
  "eventTs": "2024-01-15T10:30:00",
  "payload": {
    "userId": "user-789",
    "action": "login",
    "ipAddress": "192.168.1.1"
  },
  "source": "web-app"
}
```

**Expected Response**: `201 Created`

```json
{
  "tenantId": "tenant-123",
  "eventId": "event-456",
  "eventType": "user.action",
  "eventTs": "2024-01-15T10:30:00",
  "payload": {
    "userId": "user-789",
    "action": "login",
    "ipAddress": "192.168.1.1"
  },
  "source": "web-app",
  "status": "PENDING",
  "createdAt": "2024-01-15T10:30:05"
}
```

### 2. Get Outbox Summary

```
GET http://localhost:8080/v1/outbox/summary?tenantId=tenant-123&from=2024-01-08T00:00:00&to=2024-01-15T23:59:59
```

**Expected Response**: `200 OK`

```json
{
  "tenantId": "tenant-123",
  "from": "2024-01-08T00:00:00",
  "to": "2024-01-15T23:59:59",
  "pending": 10,
  "processing": 2,
  "done": 150,
  "failed": 5,
  "total": 167
}
```

### 3. Retry Failed Events

```
POST http://localhost:8080/v1/outbox/retry?tenantId=tenant-123&limit=10
```

**Expected Response**: `200 OK`

```json
{
  "tenantId": "tenant-123",
  "retriedCount": 3,
  "message": "Retried 3 failed records"
}
```

## Testing Flow

### Happy Path Testing

1. **Ingest events**: Use "Ingest Single Event" or "Ingest Bulk Events"
2. **Verify ingestion**: Check metrics endpoint to see `events_ingested_total`
3. **Wait for processing**: Wait a few seconds for the scheduler to process
4. **Check status**: Use "Get Outbox Summary" to see processing status
5. **Verify completion**: Status should change from PENDING → PROCESSING → DONE

### Negative Testing

1. **Validation errors**: Try "Ingest Event - Missing tenantId"
2. **Not found**: Try "Get Event - Not Found"
3. **Invalid parameters**: Try "Retry Failed - Invalid Limit"

## Common Issues

### Connection Refused

- Make sure the Spring Boot application is running
- Verify the `baseUrl` variable matches your server port (default: 8080)

### 500 Internal Server Error

- Check application logs
- Verify MySQL database is running and accessible
- Verify BigQuery credentials are configured

### Schema Validation Errors

- Ensure Flyway migrations have run successfully
- Check database schema matches migration scripts

## Tips

1. **Use Correlation IDs**: Each request automatically generates a correlation ID for tracing
2. **Check Metrics**: Use the metrics endpoints to monitor service health
3. **Monitor Outbox**: Regularly check outbox summary to track processing
4. **Retry Failed**: Use retry endpoint to manually retry failed events

