ALTER TABLE parked_fire ADD COLUMN failures integer DEFAULT 0 NOT NULL;

ALTER TABLE parked_fire ADD COLUMN waits_for text;

ALTER TABLE parked_fire ADD CONSTRAINT parked_fire_failures_not_null NOT NULL failures;
