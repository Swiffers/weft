CREATE TABLE IF NOT EXISTS trigger_activation (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            -- The trigger, spelled the way the program reads it (`door`,
            -- `one.door` inside an included file): the same spelling the
            -- signal rows carry.
            trigger TEXT NOT NULL,
            -- Whose activation: NULL for the program's shared trigger,
            -- else the instance whose copy of a per-instance trigger it is.
            instance_id TEXT,
            -- registered | activating | active | deactivating | inactive
            status TEXT NOT NULL,
            accepting_fires BOOLEAN NOT NULL,
            fires_visible_to_consumers BOOLEAN NOT NULL,
            fires_deadline_unix BIGINT,
            drain_deadline_unix BIGINT,
            -- Why it is down when the infra it reads took it down rather
            -- than a person: 'health' (the health loop parked it) or
            -- 'infra' (an infra verb took it down with its copies). NULL
            -- otherwise. The copy coming back brings it back.
            -- SYNC: the spellings <-> weft_broker_client::activation::DownWith
            went_down_with TEXT,
            -- When it went down so: a copy it reads that was applied after
            -- this has come back up since, which is what brings it back.
            went_down_at_unix BIGINT,
            -- The trigger-setup run of the activation in flight; one run
            -- sets up every activation a verb names, so rows share it.
            activating_run_id UUID,
            heartbeat_unix BIGINT NOT NULL DEFAULT 0,
            -- What the listeners fire, recorded when setup finished.
            activation_program JSONB,
            activation_version TEXT,
            updated_at BIGINT NOT NULL
        );
CREATE UNIQUE INDEX IF NOT EXISTS trigger_activation_key
             ON trigger_activation (project_id, trigger, instance_id) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS trigger_activation_run_id
             ON trigger_activation (activating_run_id) WHERE activating_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS trigger_activation_transitional
             ON trigger_activation (status) WHERE status IN ('activating', 'deactivating');
DROP TRIGGER IF EXISTS trigger_activation_held_on_status ON trigger_activation;
CREATE TRIGGER trigger_activation_held_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION signal_held_notify();
CREATE OR REPLACE FUNCTION trigger_activation_routes_notify() RETURNS trigger AS $$
            DECLARE
                tenant TEXT;
            BEGIN
                SELECT p.tenant_id INTO tenant FROM project p
                    WHERE p.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.project_id ELSE NEW.project_id END;
                -- A project already gone told its tenant itself.
                IF tenant IS NOT NULL THEN
                    PERFORM pg_notify('weft_routes', tenant);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trigger_activation_routes_on_status ON trigger_activation;
CREATE TRIGGER trigger_activation_routes_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION trigger_activation_routes_notify();
DROP TRIGGER IF EXISTS trigger_activation_routes_on_row ON trigger_activation;
CREATE TRIGGER trigger_activation_routes_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION trigger_activation_routes_notify();
CREATE OR REPLACE FUNCTION trigger_activation_triggers_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trigger_activation_triggers_on_status ON trigger_activation;
CREATE TRIGGER trigger_activation_triggers_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION trigger_activation_triggers_notify();
DROP TRIGGER IF EXISTS trigger_activation_triggers_on_row ON trigger_activation;
CREATE TRIGGER trigger_activation_triggers_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION trigger_activation_triggers_notify();
DROP TRIGGER IF EXISTS trigger_activation_held_on_row ON trigger_activation;
CREATE TRIGGER trigger_activation_held_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION signal_held_notify();
