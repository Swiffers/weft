ALTER TABLE infra_health_rule ADD COLUMN queued_command bigint;

ALTER TABLE infra_health_rule ADD COLUMN queued_deadline_unix bigint;

ALTER TABLE infra_health_rule ADD COLUMN took_down boolean DEFAULT false NOT NULL;

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_took_down_not_null NOT NULL took_down;

CREATE OR REPLACE FUNCTION infra_command_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NOT NEW.drains) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                    IF NEW.verb IN ('deactivate', 'reactivate') THEN
                        PERFORM pg_notify('weft_infra_command', 'look:' || NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE INDEX idx_infra_health_rule_queued ON infra_health_rule USING btree (queued_deadline_unix) WHERE (queued_deadline_unix IS NOT NULL);
