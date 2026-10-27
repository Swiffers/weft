ALTER TABLE project_code ADD COLUMN node_needs jsonb DEFAULT '{}'::jsonb NOT NULL;
