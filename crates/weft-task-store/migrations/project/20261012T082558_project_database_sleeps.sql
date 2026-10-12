ALTER TABLE project ADD COLUMN removing boolean DEFAULT false NOT NULL;

ALTER TABLE project ADD COLUMN resync jsonb;

ALTER TABLE project ADD CONSTRAINT project_removing_not_null NOT NULL removing;
