CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
            id                BIGSERIAL PRIMARY KEY,
            tenant_id         TEXT NOT NULL,
            project_id        UUID NOT NULL,
            node_id           TEXT,
            verb              TEXT NOT NULL,
            spec_json         JSONB,
            issued_by_replica     TEXT NOT NULL,
            issued_at_unix    BIGINT NOT NULL,
            claimed_by_replica    TEXT,
            -- A dispatcher's claim lapses `CLAIM_LEASE_TTL` after this. A
            -- drain whose next look is further away sets it ahead, so the
            -- claim lasts until then
            -- (`crate::lifecycle_claimer::CommandClaim`).
            claimed_at_unix   BIGINT,
            completed_at_unix BIGINT,
            -- 'succeeded' | 'failed' | 'cancelled' | NULL.
            -- NULL = "no result yet" (pending or claimed).
            -- 'failed' = the claimer hit a real error executing the
            --   verb; the worker / caller treats this as a failure.
            -- 'cancelled' = the command was abandoned (e.g. the
            --   targeted node was removed before run). NOT a
            --   failure; surfaces as "no longer applicable".
            outcome           TEXT,
            -- Human-readable message accompanying the outcome.
            -- NULL on 'succeeded' and on still-pending rows. The
            -- error message on 'failed'; the reason on 'cancelled'.
            -- Decoded into the right typed field based on `outcome`.
            outcome_message   TEXT,
            -- Stop only: force scale-to-zero EVERY unit, ignoring each
            -- unit's `on_stop` (so a NoOp unit comes down too). The
            -- explicit "I accept the downtime, take it all down so I
            -- can update it" override. Default false.
            force             BOOLEAN NOT NULL DEFAULT FALSE,
            -- The user requested cancellation of this command while it
            -- was CLAIMED (in flight), announced as it is set
            -- (`cancel:<id>`). Whoever runs it halts (leaving
            -- per-node partial state visible; no platform applies a
            -- whole project transactionally).
            -- Pending unclaimed rows are cancelled outright (outcome =
            -- 'cancelled') instead of flagged.
            cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
            -- A stop or terminate that first waits for the running work
            -- its copies reach (`--running-policy wait`): until when
            -- (unix seconds), past which what is left is cancelled. The
            -- dispatcher's claimer waits it out (`crate::drain`), clears
            -- it, and only then does a supervisor take the command. NULL
            -- for every other command.
            drain_by_unix     BIGINT,
            -- The run that asked for the take-down (a program's own
            -- `ctx.infra(..).stop(..)`): waited for like any other, never
            -- cancelled by its drain; its own `StopSelf` decides.
            asked_by          UUID,
            -- Which copies of the infra the command acts on
            -- (`weft_core::instance::Copies`): the shared ones (instance_id
            -- NULL, every_copy FALSE), one instance's (instance_id set), or
            -- every copy there is (every_copy TRUE; the project going).
            -- An apply always names exactly one copy.
            instance_id         TEXT,
            every_copy        BOOLEAN NOT NULL DEFAULT FALSE,
            -- How many times a supervisor claimed it to run. A command that
            -- is claimed again and again never ended (its run or the write
            -- of its ending keeps failing, or its supervisor keeps dying
            -- on it), so the claim gives it up once this reaches
            -- `SUPERVISOR_GIVE_UP_AFTER` (`weft_broker::lifecycle_writes::claim_next`).
            runs              INT NOT NULL DEFAULT 0
        );
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_pending
              ON infra_lifecycle_command(tenant_id)
              WHERE completed_at_unix IS NULL;
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_supervisor_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND verb IN ('apply', 'stop', 'terminate');
CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_dispatcher_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_replica IS NULL
                AND (drain_by_unix IS NOT NULL OR verb IN ('deactivate', 'reactivate', 'upgrade'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id, instance_id) NULLS NOT DISTINCT
              WHERE completed_at_unix IS NULL AND verb = 'apply';
CREATE TABLE IF NOT EXISTS infra_health_protocol (
            -- What one health protocol of a project did, kept across the
            -- supervisor's processes
            -- (weft_broker_client::protocol::HealthProtocolState).
            project_id           UUID NOT NULL,
            protocol             TEXT NOT NULL,
            -- The copies its action acted on since it was last armed (a
            -- JSON array of {node_id, instance}); NULL: it has not fired.
            acted_json           JSONB,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            -- When its failed action is tried again (unix seconds).
            next_retry_at_unix   BIGINT,
            -- When it gave up (unix seconds), shown in `weft status`; it is
            -- armed again when the units it saw then change, a command
            -- touches the project, or every unit is healthy.
            gave_up_at_unix      BIGINT,
            -- The units the health loop saw when it gave up.
            gave_up_seen         JSONB,
            last_error           TEXT,
            PRIMARY KEY (project_id, protocol)
        );
CREATE INDEX IF NOT EXISTS idx_infra_health_protocol_retry
              ON infra_health_protocol(next_retry_at_unix)
              WHERE gave_up_at_unix IS NULL AND next_retry_at_unix IS NOT NULL;
CREATE TABLE IF NOT EXISTS infra_health_look (
            -- How the supervisor's passes over one project stand, kept
            -- across its processes (weft_broker::lifecycle_writes::sync_ownership).
            project_id      UUID PRIMARY KEY,
            -- When a person last asked for a look (`weft infra look`, unix
            -- seconds): a health protocol that gave up at or before it is
            -- armed again.
            asked_at_unix   BIGINT,
            -- Passes over the project that failed in a row since one went
            -- through (weft_broker_client::protocol::supervisor_retry_after).
            failures        INTEGER NOT NULL DEFAULT 0,
            -- When a pass tries the project again (unix seconds); NULL:
            -- nothing failed, a pass took the retry, or it gave up.
            retry_at_unix   BIGINT
        );
CREATE INDEX IF NOT EXISTS idx_infra_health_look_retry
              ON infra_health_look(retry_at_unix)
              WHERE retry_at_unix IS NOT NULL;
CREATE OR REPLACE FUNCTION infra_command_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NEW.drain_by_unix IS NULL) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS infra_command_notify_on_issue ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_issue
            AFTER INSERT ON infra_lifecycle_command
            FOR EACH ROW
            EXECUTE FUNCTION infra_command_notify();
DROP TRIGGER IF EXISTS infra_command_notify_on_drained ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_drained
            AFTER UPDATE OF drain_by_unix ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.drain_by_unix IS NULL AND OLD.drain_by_unix IS NOT NULL)
            EXECUTE FUNCTION infra_command_notify();
DROP TRIGGER IF EXISTS infra_command_notify_on_done ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_done
            AFTER UPDATE OF completed_at_unix ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.completed_at_unix IS NOT NULL AND OLD.completed_at_unix IS NULL)
            EXECUTE FUNCTION infra_command_notify();
CREATE OR REPLACE FUNCTION infra_command_cancel_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_command', 'cancel:' || NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS infra_command_notify_on_cancel ON infra_lifecycle_command;
CREATE TRIGGER infra_command_notify_on_cancel
            AFTER UPDATE OF cancel_requested ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.cancel_requested AND NOT OLD.cancel_requested)
            EXECUTE FUNCTION infra_command_cancel_notify();
