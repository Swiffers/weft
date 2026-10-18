CREATE TABLE IF NOT EXISTS infra_node (
            project_id          UUID NOT NULL,
            node_id             TEXT NOT NULL,
            copy_id             TEXT NOT NULL DEFAULT '',
            status              TEXT NOT NULL,
            failure_stage       TEXT,
            failure_message     TEXT,
            applied_spec_hash   TEXT,
            applied_at_unix     BIGINT,
            endpoints_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Endpoint name to its front-door path, for Public
            -- endpoints only. Stamped at apply.
            public_paths_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Disk names to keep on terminate. JSON array; empty means
            -- "delete every disk the copy owns".
            keep_disks_json  JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Per-unit runtime (status + resolved health windows +
            -- stop_behavior) keyed by unit name. The `status` column
            -- is a rollup over these. Stamped at apply.
            units_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Whose copy: NULL for the program's shared one, else the
            -- instance whose copy of a `@per_instance` node this is.
            instance_id         TEXT,
            -- Endpoint name to the `host:port` a SameNetwork endpoint
            -- answers at on the install's network. Stamped at apply.
            doors_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Endpoint name to where weft's own roles reach it (the
            -- workers' address unless they sit on a network weft's
            -- roles are not on). Stamped at apply.
            install_endpoints_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- What the host runs differently from what was asked (a
            -- GPU kind it cannot choose), one sentence each. JSON
            -- array. Stamped at apply.
            notes_json          JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- While an apply is under way: what it waits on, in the
            -- host's words, and when it began. Cleared when it lands.
            waiting_on          TEXT,
            provisioning_since_unix BIGINT,
            -- The node's baked outputs (`weft_core::infra::bake`), port
            -- to value: what its infra setup sent out on them, or the
            -- infra pushed since. A run that reads only these does not
            -- run the node.
            baked_json          JSONB NOT NULL DEFAULT '{}'::jsonb
        );
CREATE UNIQUE INDEX IF NOT EXISTS idx_infra_node_copy
             ON infra_node(project_id, node_id, instance_id) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_infra_node_project   ON infra_node(project_id);
CREATE OR REPLACE FUNCTION infra_node_status_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_infra_status', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_status', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS infra_node_status_on_row ON infra_node;
CREATE TRIGGER infra_node_status_on_row
            AFTER INSERT OR DELETE ON infra_node
            FOR EACH ROW
            EXECUTE FUNCTION infra_node_status_notify();
DROP TRIGGER IF EXISTS infra_node_status_on_change ON infra_node;
CREATE TRIGGER infra_node_status_on_change
            AFTER UPDATE OF status, endpoints_json, public_paths_json, baked_json ON infra_node
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.endpoints_json IS DISTINCT FROM OLD.endpoints_json
                  OR NEW.public_paths_json IS DISTINCT FROM OLD.public_paths_json
                  OR NEW.baked_json IS DISTINCT FROM OLD.baked_json)
            EXECUTE FUNCTION infra_node_status_notify();
