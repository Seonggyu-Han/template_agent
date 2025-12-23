USE crm;

CREATE TABLE IF NOT EXISTS campaign_runs (
  run_id CHAR(36) PRIMARY KEY,
  created_by VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  brief_json JSON NULL,
  channel VARCHAR(32) NULL,
  tone VARCHAR(32) NULL,
  target_json JSON NULL,
  selected_template_id VARCHAR(64) NULL,
  final_payload_json JSON NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS handoffs (
  handoff_id CHAR(36) PRIMARY KEY,
  run_id CHAR(36) NOT NULL,
  stage VARCHAR(64) NOT NULL,
  payload_json JSON NOT NULL,
  payload_version INT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL,
  INDEX idx_handoffs_run_stage_created (run_id, stage, created_at),
  CONSTRAINT fk_handoffs_run
    FOREIGN KEY (run_id) REFERENCES campaign_runs(run_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS campaign_approvals (
  approval_id CHAR(36) PRIMARY KEY,
  run_id CHAR(36) NOT NULL,
  marketer_id VARCHAR(64) NOT NULL,
  decision VARCHAR(16) NOT NULL, -- APPROVED / REJECTED
  comment TEXT NULL,
  created_at DATETIME NOT NULL,
  INDEX idx_approvals_run_created (run_id, created_at),
  CONSTRAINT fk_approvals_run
    FOREIGN KEY (run_id) REFERENCES campaign_runs(run_id)
    ON DELETE CASCADE
);


