ALTER TABLE trigger_activation ADD COLUMN cut_fingerprint text;

ALTER TABLE trigger_activation ADD COLUMN depends_on text[] DEFAULT '{}'::text[] NOT NULL;

ALTER TABLE trigger_activation ADD COLUMN went_down_by_command bigint;

ALTER TABLE trigger_activation ADD CONSTRAINT trigger_activation_depends_on_not_null NOT NULL depends_on;

CREATE INDEX trigger_activation_activating ON trigger_activation USING btree (status) WHERE (status = 'activating'::text);

-- Throws away what is in trigger_activation.drain_deadline_unix. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE trigger_activation DROP COLUMN drain_deadline_unix;

-- Throws away what is in trigger_activation.went_down_at_unix. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE trigger_activation DROP COLUMN went_down_at_unix;

DROP INDEX IF EXISTS trigger_activation_transitional;
