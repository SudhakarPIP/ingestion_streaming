CREATE TABLE event_inbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  event_id VARCHAR(128) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  event_ts DATETIME(3) NOT NULL,
  payload_json JSON NOT NULL,
  source VARCHAR(64) NULL,
  checksum CHAR(64) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_tenant_event (tenant_id, event_id),
  KEY idx_event_ts (tenant_id, event_ts)
);

