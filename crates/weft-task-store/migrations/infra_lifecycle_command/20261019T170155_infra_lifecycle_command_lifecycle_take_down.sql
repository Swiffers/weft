ALTER TABLE infra_lifecycle_command ADD COLUMN drains boolean DEFAULT false NOT NULL;

ALTER TABLE infra_lifecycle_command ADD COLUMN source_hash text;

ALTER TABLE infra_lifecycle_command ADD CONSTRAINT infra_lifecycle_command_drains_not_null NOT NULL drains;

CREATE OR REPLACE FUNCTION infra_command_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NOT NEW.drains) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

DROP INDEX idx_lifecycle_cmd_dispatcher_claim;
CREATE INDEX idx_lifecycle_cmd_dispatcher_claim ON infra_lifecycle_command USING btree (id) WHERE ((completed_at_unix IS NULL) AND (claimed_by_replica IS NULL) AND (drains OR (verb = ANY (ARRAY['deactivate'::text, 'reactivate'::text, 'upgrade'::text]))));

DROP TRIGGER infra_command_notify_on_drained ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_drained AFTER UPDATE OF drains ON infra_lifecycle_command FOR EACH ROW WHEN ((old.drains AND (NOT new.drains))) EXECUTE FUNCTION infra_command_notify();

-- Throws away what is in infra_lifecycle_command.asked_by. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE infra_lifecycle_command DROP COLUMN asked_by;

-- Throws away what is in infra_lifecycle_command.drain_by_unix. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE infra_lifecycle_command DROP COLUMN drain_by_unix;
