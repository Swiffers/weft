ALTER TABLE infra_unit ADD COLUMN last_restart_at_unix bigint;

ALTER TABLE infra_unit ADD COLUMN last_restart_cause text;

ALTER TABLE infra_unit ADD COLUMN last_restart_exit_code bigint;
