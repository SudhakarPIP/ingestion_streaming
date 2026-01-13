# MySQL Outbox Ingestion + BigQuery Streaming Loader

A Spring Boot service that ingests events, stores them in MySQL using an outbox pattern, asynchronously processes them via Java scheduler, and loads curated rows into BigQuery using streaming inserts.

## Features

- **RESTful Event Ingestion**: Supports single and bulk event ingestion with validation
- **Idempotency**: Enforces unique (tenant_id, event_id) constraints
- **Outbox Pattern**: Asynchronous event processing via outbox table with MySQL-safe locking
- **BigQuery Integration**: Streaming inserts for processed events with partitioning and clustering
- **Scheduled Processing**: Automatic processing of pending events with configurable batching
- **Retry Mechanism**: Exponential backoff retry logic with configurable max attempts
- **Observability**: Correlation IDs, metrics, and health endpoints
- **Production Ready**: Docker support, Jenkins CI/CD, AWS deployment configurations

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- MySQL 8.0+
- Google Cloud Platform account with BigQuery access
- GCP service account credentials (JSON file)
- Docker (optional, for containerized deployment)
- AWS CLI and credentials (for AWS deployment)

## Local Setup

### 1. MySQL Setup

Start MySQL locally:

```bash
# Using Docker
docker run --name ingestion-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=eventdb -p 3306:3306 -d mysql:8.0

# Or use existing MySQL instance
mysql -u root -p
CREATE DATABASE eventdb;
```

### 2. GCP Credentials Setup

1. Create a GCP service account with BigQuery Data Editor and Job User roles
2. Download the JSON key file
3. Set the path:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

Or place the file and reference it in `application.yml`:

```yaml
bigquery:
  credentials-path: /path/to/service-account-key.json
```

### 3. Configuration

Edit `src/main/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/eventdb?useSSL=false&serverTimezone=UTC
    username: root
    password: root

bigquery:
  project-id: your-gcp-project-id
  dataset: analytics
  table: events_curated
  credentials-path: ${GOOGLE_APPLICATION_CREDENTIALS:}

worker:
  batch-size: 200
  lock-ttl-seconds: 300
  instance-id: local-instance
  max-retry-count: 5
```

### 4. Run Locally

```bash
# Build
mvn clean package

# Run
mvn spring-boot:run

# Or run jar
java -jar target/ingestion-streaming-1.0.0.jar
```

The service starts on port 8080. Flyway will automatically run migrations to create the database tables.

## Database Schema

Tables are created automatically via Flyway migrations:

### event_inbox

Stores immutable incoming event data.

- `id`: BIGINT (Primary Key)
- `tenant_id`: VARCHAR(64) NOT NULL
- `event_id`: VARCHAR(128) NOT NULL
- `event_type`: VARCHAR(64) NOT NULL
- `event_ts`: DATETIME(3) NOT NULL
- `payload_json`: JSON NOT NULL
- `source`: VARCHAR(64) NULL
- `checksum`: CHAR(64) NULL (SHA-256 hash)
- `created_at`: DATETIME(3) NOT NULL
- `updated_at`: DATETIME(3) NOT NULL
- **Unique Constraint**: `uk_tenant_event` on (tenant_id, event_id)
- **Index**: `idx_event_ts` on (tenant_id, event_ts)

### event_outbox

Tracks async processing state.

- `id`: BIGINT (Primary Key)
- `tenant_id`: VARCHAR(64) NOT NULL
- `event_id`: VARCHAR(128) NOT NULL
- `status`: VARCHAR(16) NOT NULL (PENDING, PROCESSING, DONE, FAILED)
- `retry_count`: INT NOT NULL DEFAULT 0
- `next_retry_at`: DATETIME(3) NOT NULL
- `last_error`: VARCHAR(2048) NULL
- `locked_by`: VARCHAR(64) NULL
- `locked_at`: DATETIME(3) NULL
- `created_at`: DATETIME(3) NOT NULL
- `updated_at`: DATETIME(3) NOT NULL
- **Unique Constraint**: `uk_outbox_item` on (tenant_id, event_id)
- **Indexes**: `idx_pick` on (status, next_retry_at, locked_at), `idx_tenant_created` on (tenant_id, created_at)

## API Usage Examples

### 1. Ingest Single Event

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Correlation-Id: req-123" \
  -d '{
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
  }'
```

**Response:**
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

### 2. Bulk Ingest Events

```bash
curl -X POST http://localhost:8080/v1/events/bulk \
  -H "Content-Type: application/json" \
  -d '[
    {
      "tenantId": "tenant-123",
      "eventId": "event-001",
      "eventType": "user.action",
      "eventTs": "2024-01-15T10:30:00",
      "payload": {"action": "click"},
      "source": "web"
    },
    {
      "tenantId": "tenant-123",
      "eventId": "event-002",
      "eventType": "user.action",
      "eventTs": "2024-01-15T10:30:01",
      "payload": {"action": "view"},
      "source": "web"
    }
  ]'
```

Returns array of responses, one per event.

### 3. Get Event

```bash
curl http://localhost:8080/v1/events/tenant-123/event-456
```

### 4. Outbox Summary

```bash
curl "http://localhost:8080/v1/outbox/summary?tenantId=tenant-123&from=2024-01-08T00:00:00&to=2024-01-15T23:59:59"
```

**Response:**
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

### 5. Retry Failed Events

```bash
curl -X POST "http://localhost:8080/v1/outbox/retry?tenantId=tenant-123&limit=10"
```

**Response:**
```json
{
  "tenantId": "tenant-123",
  "retriedCount": 3,
  "message": "Retried 3 failed records"
}
```

## Worker Behavior

### Claim-Then-Fetch Pattern

The worker uses MySQL-safe locking (no SKIP LOCKED) with an atomic claim pattern:

1. **Claim Rows**: Atomically updates PENDING rows to PROCESSING with instance lock
   ```sql
   UPDATE event_outbox
   SET status='PROCESSING', locked_by=?, locked_at=NOW(3)
   WHERE status='PENDING'
     AND next_retry_at <= NOW(3)
     AND (locked_at IS NULL OR locked_at < (NOW(3) - INTERVAL ? SECOND))
   ORDER BY id LIMIT ?
   ```

2. **Fetch Claimed Rows**: Retrieves rows locked by current instance
   ```sql
   SELECT id, tenant_id, event_id
   FROM event_outbox
   WHERE status='PROCESSING'
     AND locked_by=?
     AND locked_at >= (NOW(3) - INTERVAL ? SECOND)
   ```

3. **Process Each Row**: Loads event from inbox, streams to BigQuery, updates status

### Lock TTL (Time-To-Live)

- **Purpose**: Recover stuck PROCESSING rows if an instance crashes
- **Default**: 300 seconds (5 minutes)
- **Behavior**: After TTL expires, another instance can claim the row
- **Configuration**: `worker.lock-ttl-seconds`

### Retry Logic with Exponential Backoff

Failed records are retried with exponential backoff:

- **Retry 0**: +1 minute
- **Retry 1**: +5 minutes
- **Retry 2**: +15 minutes
- **Retry 3+**: +60 minutes
- **Max Retries**: 5 (configurable via `worker.max-retry-count`)

After max retries, records remain FAILED until manual retry via API.

### Status Transitions

- **PENDING → PROCESSING**: Worker claims the row
- **PROCESSING → DONE**: Successful BigQuery insertion
- **PROCESSING → FAILED**: Error during processing (scheduled for retry)
- **FAILED → PENDING**: Manual retry via `/v1/outbox/retry` endpoint

### Concurrent Processing

Multiple instances can run in parallel:
- Each instance has unique `instance-id` (hostname/pod name)
- Lock TTL prevents double-processing
- Batch size limits concurrent processing per instance

### Configuration

```yaml
worker:
  batch-size: 200              # Records per batch
  lock-ttl-seconds: 300        # Lock expiration time
  instance-id: ${HOSTNAME}     # Unique instance identifier
  max-retry-count: 5           # Maximum retry attempts

scheduler:
  outbox:
    enabled: true
    fixed-delay: 5000          # Milliseconds between scheduler runs
    initial-delay: 10000       # Initial delay before first run
```

## BigQuery Integration

### Table Schema

The service automatically creates the BigQuery table `analytics.events_curated`:

- `tenant_id`: STRING (REQUIRED)
- `event_id`: STRING (REQUIRED)
- `event_type`: STRING (REQUIRED)
- `event_ts`: TIMESTAMP (REQUIRED)
- `event_date`: DATE (REQUIRED) - **Partition field**
- `source`: STRING (NULLABLE)
- `payload_hash`: STRING (REQUIRED) - SHA-256 hash of payload
- `payload_json`: STRING (REQUIRED) - Full JSON payload
- `ingested_at`: TIMESTAMP (REQUIRED)

**Partitioning**: By `event_date` (daily partitions)
**Clustering**: By `tenant_id` (for efficient tenant-based queries)

### Setup Notes

1. **Create GCP Service Account**:
   - Roles: BigQuery Data Editor, BigQuery Job User
   - Download JSON key

2. **Configure Credentials**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   ```

3. **Table Creation**:
   - Table is created automatically on first insert
   - Ensure dataset exists or has permissions to create

4. **Query Example**:
   ```sql
   SELECT *
   FROM `analytics.events_curated`
   WHERE event_date = '2024-01-15'
     AND tenant_id = 'tenant-123'
   LIMIT 100
   ```

## Observability

### Correlation IDs

All requests include a correlation ID for request tracing:

- **Header**: `X-Correlation-Id` (auto-generated if not provided)
- **Logs**: Included in every log entry via MDC
- **Response**: Included in error responses

### Metrics

Available at `/actuator/metrics`:

- `events_ingested_total`: Counter of ingested events
- `outbox_claimed_total`: Counter of claimed outbox records
- `outbox_done_total`: Counter of successfully processed records
- `outbox_failed_total`: Counter of failed records
- `bigquery_insert_success_total`: Counter of successful BigQuery inserts
- `bigquery_insert_failure_total`: Counter of failed BigQuery inserts

**Prometheus Format**: `/actuator/prometheus`

### Health Endpoints

- **Health Check**: `GET /actuator/health`
  ```json
  {
    "status": "UP",
    "components": {
      "db": {"status": "UP"},
      "diskSpace": {"status": "UP"},
      "ping": {"status": "UP"}
    }
  }
  ```

- **Metrics**: `GET /actuator/metrics`
- **Prometheus**: `GET /actuator/prometheus`

### Log Structure

Logs include structured fields:
```
2024-01-15 10:30:05.123 [http-nio-8080-exec-1] INFO  [abc123] [tenant-123] [event-456] [789] EventService - Event saved
```

Format: `[correlationId] [tenantId] [eventId] [outboxId]`

## Failure Scenarios & Operations

### Scenario 1: Transient BigQuery Failure

**Symptoms**: Records in PROCESSING status, errors in logs

**Resolution**:
1. Check error logs for BigQuery issues
2. Wait for automatic retry (exponential backoff)
3. Monitor metrics: `bigquery_insert_failure_total`
4. If persistent, check GCP quotas and permissions

### Scenario 2: Stuck PROCESSING Records

**Symptoms**: Records stuck in PROCESSING status

**Cause**: Instance crash before completion

**Resolution**:
1. Wait for lock TTL expiration (default 5 minutes)
2. Another instance will automatically claim and process
3. Monitor: `outbox_claimed_total` metrics

### Scenario 3: Exceeded Max Retries

**Symptoms**: Records in FAILED status with `retry_count >= 5`

**Resolution**:
1. Check error message: `GET /v1/events/{tenantId}/{eventId}`
2. Fix root cause (schema mismatch, invalid data, etc.)
3. Manual retry: `POST /v1/outbox/retry?tenantId={tenantId}&limit={limit}`
4. Monitor: Outbox summary endpoint

### Scenario 4: Database Connection Issues

**Symptoms**: Health check fails, no records processed

**Resolution**:
1. Check `/actuator/health` endpoint
2. Verify MySQL connectivity
3. Check connection pool settings
4. Review database logs

### Operational Commands

```bash
# Check health
curl http://localhost:8080/actuator/health

# View metrics
curl http://localhost:8080/actuator/metrics/events_ingested_total

# Get outbox summary
curl "http://localhost:8080/v1/outbox/summary?tenantId=tenant-123"

# Retry failed events
curl -X POST "http://localhost:8080/v1/outbox/retry?tenantId=tenant-123&limit=100"

# View Prometheus metrics
curl http://localhost:8080/actuator/prometheus
```

## Docker Deployment

### Build Image

```bash
docker build -t ingestion-streaming:latest .
```

### Run with Docker Compose

```bash
docker-compose up -d
```

This starts:
- MySQL on port 3306
- Application on port 8080

### Environment Variables

```bash
docker run -p 8080:8080 \
  -e SPRING_DATASOURCE_URL=jdbc:mysql://mysql:3306/eventdb \
  -e SPRING_DATASOURCE_USERNAME=root \
  -e SPRING_DATASOURCE_PASSWORD=root \
  -e BIGQUERY_PROJECT_ID=your-project \
  -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  -v /path/to/key.json:/path/to/key.json:ro \
  ingestion-streaming:latest
```

## AWS Deployment

### ECS Deployment

1. **Build and Push to ECR**:
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
   docker build -t ingestion-streaming .
   docker tag ingestion-streaming:latest ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/ingestion-streaming:latest
   docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/ingestion-streaming:latest
   ```

2. **Create Task Definition**:
   - Update `aws/task-definition.json` with your account ID and region
   - Configure secrets in AWS Secrets Manager/SSM
   - Create task definition:
     ```bash
     aws ecs register-task-definition --cli-input-json file://aws/task-definition.json
     ```

3. **Create ECS Service**:
   ```bash
   aws ecs create-service --cli-input-json file://aws/ecs-service.json
   ```

4. **Configure ALB Health Check**:
   - Target Group: `/actuator/health`
   - Health Check Interval: 30s
   - Healthy Threshold: 2
   - Unhealthy Threshold: 3

### Secrets Management

Store secrets in AWS Secrets Manager or SSM Parameter Store:

- MySQL password: `dev/mysql/password`
- BigQuery credentials: `dev/bigquery/credentials`
- BigQuery project ID: `dev/bigquery/project-id` (SSM Parameter)

### Health Checks

- **ECS Task Health**: Uses `/actuator/health` endpoint
- **ALB Target Group**: Configured to check `/actuator/health`
- **Container Health**: Defined in task definition

## Jenkins CI/CD

The `Jenkinsfile` includes:

1. **Build**: Compile and package
2. **Unit Tests**: Run tests with coverage
3. **Code Coverage**: Jacoco report (minimum 70%)
4. **Docker Build**: Build Docker image
5. **Docker Push**: Push to ECR
6. **Deploy**: Update ECS service
7. **Health Check**: Verify deployment

### Setup

1. Configure Jenkins credentials:
   - `aws-account-id`: AWS account ID
   - AWS credentials in Jenkins credentials store

2. Update Jenkinsfile:
   - Set `AWS_REGION`
   - Set `ECS_CLUSTER` and `ECS_SERVICE`
   - Update ALB endpoint for health check

3. Run pipeline:
   ```bash
   jenkins build
   ```

## Testing

### Unit Tests

```bash
mvn test
```

### Code Coverage

```bash
mvn jacoco:report
# View report: target/site/jacoco/index.html
```

### Integration Tests

Use Testcontainers for MySQL integration tests (example structure):

```java
@Testcontainers
class EventServiceIntegrationTest {
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");
    
    // Test implementation
}
```

## Error Handling

All errors follow a standardized schema:

```json
{
  "error": "Validation error",
  "message": "tenantId: must not be blank",
  "timestamp": "2024-01-15T10:30:00",
  "path": "/v1/events",
  "correlationId": "abc-123-def"
}
```

Error codes:
- `400`: Validation/Bad Request
- `404`: Not Found
- `500`: Internal Server Error

## Limitations

- Maximum retry count: 5 (configurable)
- BigQuery streaming inserts subject to GCP quotas
- Lock TTL: 5 minutes default (configurable)
- Batch processing is sequential within each batch

## Future Enhancements

- Dead letter queue for permanently failed events
- Batch BigQuery inserts for better throughput
- Distributed tracing integration (Zipkin/Jaeger)
- Advanced monitoring dashboards
- Rate limiting per tenant
