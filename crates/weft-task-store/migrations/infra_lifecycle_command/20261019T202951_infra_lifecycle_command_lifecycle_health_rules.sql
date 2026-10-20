CREATE TABLE infra_health_rule (
    consecutive_failures integer DEFAULT 0 NOT NULL,
    fired boolean DEFAULT false NOT NULL,
    gave_up_at_unix bigint,
    gave_up_seen jsonb,
    instance_id text,
    last_error text,
    next_retry_at_unix bigint,
    node_id text NOT NULL,
    project_id uuid NOT NULL,
    rule text NOT NULL
);

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_consecutive_failures_not_null NOT NULL consecutive_failures;

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_fired_not_null NOT NULL fired;

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_node_id_not_null NOT NULL node_id;

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_project_id_not_null NOT NULL project_id;

ALTER TABLE infra_health_rule ADD CONSTRAINT infra_health_rule_rule_not_null NOT NULL rule;

CREATE UNIQUE INDEX idx_infra_health_rule_key ON infra_health_rule USING btree (project_id, node_id, instance_id, rule) NULLS NOT DISTINCT;

CREATE INDEX idx_infra_health_rule_retry ON infra_health_rule USING btree (next_retry_at_unix) WHERE ((gave_up_at_unix IS NULL) AND (next_retry_at_unix IS NOT NULL));

-- Throws away every row in infra_health_protocol.
DROP TABLE infra_health_protocol;
