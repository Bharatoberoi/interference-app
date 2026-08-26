CREATE DATABASE IF NOT EXISTS a6_poc CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE a6_poc;

CREATE TABLE IF NOT EXISTS local_service_credentials (
  service_name VARCHAR(32) PRIMARY KEY,
  username VARCHAR(64) NOT NULL,
  password_value VARCHAR(128) NOT NULL,
  environment VARCHAR(16) NOT NULL DEFAULT 'local',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO local_service_credentials(service_name, username, password_value)
VALUES
 ('jims', 'jims-local', 'JimsLocal!2026'),
 ('mmp', 'mmp-local', 'MmpLocal!2026'),
 ('hpsm', 'hpsm-local', 'HpsmLocal!2026')
ON DUPLICATE KEY UPDATE username=VALUES(username), password_value=VALUES(password_value);
