CREATE TABLE IF NOT EXISTS project (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                -- Free-text project description (metadata only; never affects
                -- the graph, build, or runtime). Set at create time on the
                -- website; empty string when unset (NOT NULL keeps reads simple).
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                project_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                running_binary_hash TEXT,
                running_definition_hash TEXT,
                running_infra_hash TEXT,
                running_source JSONB,
                tenant_id TEXT NOT NULL,
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- The project's own worker levers, each one it sets
                -- replacing the install's (`WorkerOverrides`); empty
                -- runs on the install's.
                worker_settings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- Per-project health protocols overriding the weft
                -- default. NULL = use default. Schema per
                -- weft_infra_supervisor::protocol::HealthProtocols.
                health_protocols_json JSONB,
                -- Verb-transition marker, orthogonal to `status`:
                -- 'none' | 'building' | 'cancelling_build' | 'resyncing'.
                -- Written only by its own single-flight methods
                -- (try_begin_building / try_begin_resyncing /
                -- request_cancel_build / settle_transition), never by
                -- lifecycle writes, so a deactivate can't stomp an
                -- in-flight marker. A build's is held by the request
                -- starting the builds, then by the project's waiting
                -- version until it ends; a resync's by its plan (`resync`).
                transition TEXT NOT NULL DEFAULT 'none',
                -- Heartbeat for a build transition: the request starting
                -- its builds bumps this on an interval and zeroes it when it
                -- lets go; once it is stale, only a waiting version holds
                -- it (settle_transition).
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0,
                -- The project's resync (`crate::task_kinds::resync::ResyncPlan`):
                -- the drive under way, whose triggers it takes down and
                -- brings back, with the person's take-down choice and the
                -- build it brings live, the owners it already brought
                -- back, and why it stopped when it did. Under way while
                -- `transition` is 'resyncing'; once its task ended it, or a
                -- person gave it up (`given_up`: when, and the triggers it
                -- left down, which `weft status` shows), kept as that
                -- drive's record until the next resync.
                resync JSONB,
                -- The version tree's HEAD (`crate::versions`): the version
                -- the next checkpoint or run parents on, the run the next
                -- `--seed` inherits from (NULL when head is a bare
                -- version). Moved by checkpoint, run and branch; nothing
                -- lives on disk.
                head_version TEXT,
                head_run UUID,
                -- The project's own port on a local install
                -- (`crate::project_ports`): given at its first activation,
                -- or named with `weft activate --port`, and kept, moved to a free one when another program took
                -- it. NULL until then, and on a platform that gives
                -- projects no port.
                api_port INTEGER,
                -- Where the project's front answers its callers, or why it
                -- cannot (`weft_core::projects::ProjectAddress`, written by
                -- `crate::front`): what `weft status` shows and where the
                -- listener hands the project's events. NULL while nothing
                -- of the project takes work.
                api_address JSONB,
                -- The order of the project's build asks: each build request
                -- takes the next one as it arrives, before it compiles
                -- (`next_build_ask`), from this row, so every replica
                -- agrees on it.
                build_asks BIGINT NOT NULL DEFAULT 0,
                -- The ask whose version the project runs: a registration
                -- lands only over an older ask
                -- (`crate::build::waiting::settle`).
                registered_ask BIGINT NOT NULL DEFAULT 0,
                -- `weft rm` started on it (`mark_removing`, its first
                -- write): no run of it is born from then on (the run
                -- births check it in the statement that inserts the run,
                -- `weft_record_batch` and `weft_journal::record::insert_queued_in`),
                -- so the take-down that follows reaches every run it has.
                removing BOOLEAN NOT NULL DEFAULT FALSE
            );
CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_api_port ON project(api_port) WHERE api_port IS NOT NULL;
DROP TRIGGER IF EXISTS project_routes_on_row ON project;
CREATE TRIGGER project_routes_on_row
            AFTER INSERT OR DELETE ON project
            FOR EACH ROW
            EXECUTE FUNCTION routes_notify_tenant();
CREATE OR REPLACE FUNCTION project_worker_settings_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_worker_settings', OLD.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS project_worker_settings_on_change ON project;
CREATE TRIGGER project_worker_settings_on_change
            AFTER UPDATE OF worker_settings_json ON project
            FOR EACH ROW
            WHEN (NEW.worker_settings_json IS DISTINCT FROM OLD.worker_settings_json)
            EXECUTE FUNCTION project_worker_settings_notify();
DROP TRIGGER IF EXISTS project_worker_settings_on_delete ON project;
CREATE TRIGGER project_worker_settings_on_delete
            AFTER DELETE ON project
            FOR EACH ROW
            EXECUTE FUNCTION project_worker_settings_notify();
CREATE OR REPLACE FUNCTION project_declared_infra_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_status', NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS project_declared_infra_on_change ON project;
CREATE TRIGGER project_declared_infra_on_change
            AFTER UPDATE OF project_json ON project
            FOR EACH ROW
            WHEN (NEW.project_json IS DISTINCT FROM OLD.project_json)
            EXECUTE FUNCTION project_declared_infra_notify();
CREATE TABLE IF NOT EXISTS project_code (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            binary_hash TEXT NOT NULL,
            implementations JSONB NOT NULL,
            PRIMARY KEY (project_id, binary_hash)
        );
CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, definition_hash)
            -- No foreign key to `project`: the project's removal erases
            -- its runs first and drops these in its own transaction
            -- (`ProjectStoreOps::remove`), so no run is ever left without
            -- the program it ran against.
        );
