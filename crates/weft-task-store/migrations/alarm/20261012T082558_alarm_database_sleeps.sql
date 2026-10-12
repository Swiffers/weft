ALTER TABLE alarm ADD COLUMN answered integer DEFAULT 0 NOT NULL;

ALTER TABLE alarm ADD CONSTRAINT alarm_answered_not_null NOT NULL answered;
