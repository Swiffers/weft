ALTER TABLE role_loop_due ADD COLUMN failure_written boolean DEFAULT false NOT NULL;

ALTER TABLE role_loop_due ADD COLUMN failures integer DEFAULT 0 NOT NULL;

ALTER TABLE role_loop_due ADD COLUMN seq bigint DEFAULT 0 NOT NULL;

ALTER TABLE role_loop_due ADD COLUMN set_again integer DEFAULT 0 NOT NULL;

ALTER TABLE role_loop_due ADD CONSTRAINT role_loop_due_failure_written_not_null NOT NULL failure_written;

ALTER TABLE role_loop_due ADD CONSTRAINT role_loop_due_failures_not_null NOT NULL failures;

ALTER TABLE role_loop_due ADD CONSTRAINT role_loop_due_seq_not_null NOT NULL seq;

ALTER TABLE role_loop_due ADD CONSTRAINT role_loop_due_set_again_not_null NOT NULL set_again;
