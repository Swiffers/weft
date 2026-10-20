ALTER TABLE project_code ADD COLUMN left_out jsonb DEFAULT '[]'::jsonb NOT NULL;

ALTER TABLE project_code ADD COLUMN worker_log text;

ALTER TABLE project_code ADD CONSTRAINT project_code_left_out_not_null NOT NULL left_out;
