-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE infra_node ADD COLUMN health_rules_json_version integer DEFAULT 1 NOT NULL;

CREATE TABLE infra_unit (
    flaky_after_seconds bigint NOT NULL,
    image_refs text[] DEFAULT '{}'::text[] NOT NULL,
    instance_id text,
    node_id text NOT NULL,
    not_ready_since_unix bigint,
    project_id uuid NOT NULL,
    ready_since_unix bigint,
    recovery_after_seconds bigint NOT NULL,
    status text NOT NULL,
    stop_behavior text NOT NULL,
    unit text NOT NULL
);

CREATE OR REPLACE FUNCTION infra_node_units_delete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                DELETE FROM infra_unit u
                 WHERE u.project_id = OLD.project_id AND u.node_id = OLD.node_id
                   AND u.instance_id IS NOT DISTINCT FROM OLD.instance_id;
                RETURN NULL;
            END;
            $function$
;

CREATE UNIQUE INDEX idx_infra_unit_key ON infra_unit USING btree (project_id, node_id, instance_id, unit) NULLS NOT DISTINCT;

CREATE TRIGGER infra_node_units_on_delete AFTER DELETE ON infra_node FOR EACH ROW EXECUTE FUNCTION infra_node_units_delete();
