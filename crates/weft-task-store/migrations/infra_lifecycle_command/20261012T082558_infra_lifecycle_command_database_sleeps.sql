CREATE TABLE infra_health_look (
    asked_at_unix bigint,
    failures integer DEFAULT 0 NOT NULL,
    project_id uuid NOT NULL,
    retry_at_unix bigint
);

CREATE TABLE infra_health_protocol (
    acted_json jsonb,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    gave_up_at_unix bigint,
    gave_up_seen jsonb,
    last_error text,
    next_retry_at_unix bigint,
    project_id uuid NOT NULL,
    protocol text NOT NULL
);

ALTER TABLE infra_lifecycle_command ADD COLUMN runs integer DEFAULT 0 NOT NULL;

ALTER TABLE infra_health_look ADD CONSTRAINT infra_health_look_failures_not_null NOT NULL failures;

ALTER TABLE infra_health_look ADD CONSTRAINT infra_health_look_pkey PRIMARY KEY (project_id);

ALTER TABLE infra_health_look ADD CONSTRAINT infra_health_look_project_id_not_null NOT NULL project_id;

ALTER TABLE infra_health_protocol ADD CONSTRAINT infra_health_protocol_consecutive_failures_not_null NOT NULL consecutive_failures;

ALTER TABLE infra_health_protocol ADD CONSTRAINT infra_health_protocol_pkey PRIMARY KEY (project_id, protocol);

ALTER TABLE infra_health_protocol ADD CONSTRAINT infra_health_protocol_project_id_not_null NOT NULL project_id;

ALTER TABLE infra_health_protocol ADD CONSTRAINT infra_health_protocol_protocol_not_null NOT NULL protocol;

ALTER TABLE infra_lifecycle_command ADD CONSTRAINT infra_lifecycle_command_runs_not_null NOT NULL runs;

CREATE OR REPLACE FUNCTION infra_command_cancel_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_infra_command', 'cancel:' || NEW.id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE INDEX idx_infra_health_look_retry ON infra_health_look USING btree (retry_at_unix) WHERE (retry_at_unix IS NOT NULL);

CREATE INDEX idx_infra_health_protocol_retry ON infra_health_protocol USING btree (next_retry_at_unix) WHERE ((gave_up_at_unix IS NULL) AND (next_retry_at_unix IS NOT NULL));

CREATE TRIGGER infra_command_notify_on_cancel AFTER UPDATE OF cancel_requested ON infra_lifecycle_command FOR EACH ROW WHEN ((new.cancel_requested AND (NOT old.cancel_requested))) EXECUTE FUNCTION infra_command_cancel_notify();
