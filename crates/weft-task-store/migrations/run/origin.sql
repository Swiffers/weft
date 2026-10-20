CREATE TABLE IF NOT EXISTS trigger_setup (
            project_id UUID NOT NULL,
            run_id UUID PRIMARY KEY
        );
CREATE INDEX IF NOT EXISTS idx_trigger_setup_project ON trigger_setup(project_id);
CREATE TABLE IF NOT EXISTS trigger_bake (
            project_id UUID NOT NULL,
            instance_id TEXT,
            program_hash TEXT NOT NULL,
            bake_json TEXT NOT NULL
        );
CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_bake_key
             ON trigger_bake(project_id, instance_id, program_hash) NULLS NOT DISTINCT;
CREATE TABLE IF NOT EXISTS run (
            run_id UUID PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- fire | trigger_setup | infra_setup | ... (`Phase::as_str`)
            phase TEXT NOT NULL,
            -- project | node_test (`RunKind`)
            kind TEXT NOT NULL,
            -- fast | durable (`Keeping`)
            keeping TEXT NOT NULL,
            -- false: a run that keeps no record, written down only because
            -- it failed, reported a cost or stored a file.
            recorded BOOLEAN NOT NULL,
            entry_node TEXT,
            instance_id TEXT,
            fired_by TEXT,
            source_version TEXT,
            -- NULL for a run of no program (a node test).
            definition_hash TEXT,
            binary_hash TEXT,
            -- The digest of its selection (`run_selection`), NULL for a run
            -- of the whole program.
            selection TEXT,
            -- The run it was seeded from (`weft run --seed`), the places
            -- it ran again rather than inherit, what it was asked to run
            -- (`weft_core::run_spec::RunSpec`, as resolved) and the saved
            -- example it came from: what the version tree shows of a run
            -- started by hand. NULL and empty for every other run.
            seed_of UUID,
            stale TEXT[] NOT NULL DEFAULT '{}',
            spec JSONB,
            example TEXT,
            -- running | parked | queued | ended
            state TEXT NOT NULL,
            -- The worker replica driving it, while it runs.
            owner TEXT,
            -- The owner's fencing token: raised by every claim and by the
            -- lost-run sweep, so a batch from an owner that lost the run is
            -- refused.
            epoch INTEGER NOT NULL DEFAULT 1,
            -- The `seq` of its last record row.
            last_seq INTEGER NOT NULL,
            -- How many times it was handed to a worker, this time included.
            attempts INTEGER NOT NULL DEFAULT 1,
            -- Until when (unix seconds) the run counts as delivered to a
            -- worker: a queued run handed out and not claimed yet (or
            -- waiting to be tried again), and a running run claimed or born
            -- by a worker whose lease may not be written yet.
            delivered_until BIGINT,
            -- A queued run's `delivered_until` is a wait before it is
            -- tried again (its workers refused it, were still starting, or
            -- could not be reached), not a hand-off out right now: what a
            -- change of the project's worker levers cuts short.
            waits BOOLEAN NOT NULL DEFAULT FALSE,
            -- How many times its workers refused it since it was last
            -- claimed (`weft_dispatcher::delivery`); the last refusal ends
            -- it, and the project's worker levers changing starts the count
            -- again.
            refusals SMALLINT NOT NULL DEFAULT 0,
            -- How many of its hand-offs got no answer since it was last
            -- claimed; the last stops its tries, its `delivered_until` then
            -- saying when, until something that can fix it starts them
            -- again (`weft_dispatcher::delivery::UNREACHED_TO_STOP`).
            unreached SMALLINT NOT NULL DEFAULT 0,
            -- When its workers first answered it busy since it was last
            -- claimed (unix seconds); NULL while they have not. Its pause
            -- before the next try grows with it, and past the install's
            -- alert delay it is written down for `weft status`
            -- (`weft_dispatcher::delivery::busy`).
            busy_since BIGINT,
            -- Its workers answered it busy, refused it or could not be
            -- reached with it since it was last claimed. Unlike the three
            -- counts above, its tries starting again (a worker of the
            -- project coming alive, a build registering) leave it set: it
            -- answers "did this run that leaves the queue carry a failure
            -- of its delivery", which ends what `weft status` shows of the
            -- project's workers (`weft_task_store::unanswered::delivery_waits_ended`).
            -- A claim clears it, as do its ending and a change of the
            -- project's worker levers, so only a queued run carries it.
            waited_on_workers BOOLEAN NOT NULL DEFAULT FALSE,
            -- A cancel waiting for its owner to write the run's ending.
            cancel_requested JSONB,
            -- It holds resume signals the dispatcher has to take down now
            -- that it ended (set by its ending, cleared once handled).
            holds_signals BOOLEAN NOT NULL DEFAULT FALSE,
            -- Somebody waits on its ending (a trigger setup, baked from
            -- it); cleared once the dispatcher handled it.
            watch_end BOOLEAN NOT NULL DEFAULT FALSE,
            started_at BIGINT NOT NULL,
            ended_at BIGINT,
            -- completed | failed | cancelled
            outcome TEXT,
            error TEXT,
            cancel_cause JSONB,
            -- Node firings it skipped.
            skipped INTEGER NOT NULL DEFAULT 0,
            -- It stored files of its own, which its ending's sweep reclaims.
            wrote_files BOOLEAN NOT NULL DEFAULT FALSE,
            -- What its metered calls cost, in micro-dollars.
            cost_micro_usd BIGINT NOT NULL DEFAULT 0,
            -- How long it is kept once it ended, in seconds (NULL: for
            -- ever), and the moment that runs out, set when it ends.
            keep_for BIGINT,
            keep_until BIGINT
        ) WITH (fillfactor = 85);
CREATE INDEX IF NOT EXISTS run_listing ON run (project_id, started_at DESC);
CREATE INDEX IF NOT EXISTS run_live ON run (project_id) WHERE state <> 'ended';
CREATE INDEX IF NOT EXISTS run_in_flight ON run (owner) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS run_queued ON run (started_at) WHERE state = 'queued';
CREATE OR REPLACE FUNCTION worker_lease_workers_back_notify() RETURNS trigger AS $$
            BEGIN
                IF EXISTS (SELECT 1 FROM run r WHERE r.project_id = NEW.project_id AND r.state = 'queued' AND r.unreached >= 3) THEN
                    PERFORM pg_notify('weft_workers_back', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS worker_lease_workers_back_on_insert ON worker_lease;
CREATE TRIGGER worker_lease_workers_back_on_insert
            AFTER INSERT ON worker_lease
            FOR EACH ROW
            EXECUTE FUNCTION worker_lease_workers_back_notify();
DROP TRIGGER IF EXISTS worker_lease_workers_back_on_renewal ON worker_lease;
CREATE TRIGGER worker_lease_workers_back_on_renewal
            AFTER UPDATE OF leased_until_unix ON worker_lease
            FOR EACH ROW
            WHEN (OLD.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT)
            EXECUTE FUNCTION worker_lease_workers_back_notify();
CREATE INDEX IF NOT EXISTS run_expiry ON run (keep_until) WHERE state = 'ended';
CREATE INDEX IF NOT EXISTS run_end_unhandled ON run (project_id)
             WHERE state = 'ended' AND (watch_end OR holds_signals);
CREATE TABLE IF NOT EXISTS run_log (
            run_id UUID NOT NULL,
            seq INTEGER NOT NULL,
            events BYTEA NOT NULL,
            -- The replica that wrote it.
            writer TEXT NOT NULL,
            written_at BIGINT NOT NULL,
            PRIMARY KEY (run_id, seq)
        );
ALTER TABLE run_log ALTER COLUMN events SET STORAGE EXTERNAL;
CREATE TABLE IF NOT EXISTS run_selection (
            digest TEXT PRIMARY KEY,
            selection JSONB NOT NULL
        );
CREATE TABLE IF NOT EXISTS signal_token (
            id UUID PRIMARY KEY,
            -- sha256 hex of the full token value. The raw value is NEVER
            -- stored (show-once): lookups hash the presented credential.
            token_hash TEXT NOT NULL UNIQUE,
            -- Display prefix ("wft-<word>-…") so lists can tell tokens apart.
            recognizer TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            name TEXT,
            allowed_projects UUID[] NOT NULL DEFAULT '{}',
            allowed_tags TEXT[] NOT NULL DEFAULT '{}',
            -- The display dimension, which does NOT follow the
            -- empty-means-wildcard rule above: a display can be a
            -- credential, so a token says nothing about displays and
            -- reads none. `all_displays` is the wildcard, set by
            -- `weft token mint --displays`.
            allowed_displays TEXT[] NOT NULL DEFAULT '{}',
            all_displays BOOLEAN NOT NULL DEFAULT FALSE,
            created_at BIGINT NOT NULL,
            -- An instance token: the one instance it acts inside, in its one
            -- project (`allowed_projects` holds exactly that one). NULL for a
            -- token scoped to no instance.
            instance_id TEXT,
            -- When it stops working (unix seconds); NULL never. An
            -- instance token always has one: it lives in a browser.
            expires_at BIGINT,
            -- What the token may do: `caller` (the scoped outside
            -- credential above, an instance's included) or `operator` (the
            -- tenant's admin key, which carries no scope and acts as no
            -- instance). Checked at every door.
            -- SYNC: kind values <-> weft_core::signal_token::TokenKind::as_str
            kind TEXT NOT NULL DEFAULT 'caller' CHECK (kind IN ('caller', 'operator')),
            CONSTRAINT signal_token_instance_has_one_project
                CHECK (instance_id IS NULL OR cardinality(allowed_projects) = 1),
            CONSTRAINT signal_token_instance_expires
                CHECK (instance_id IS NULL OR expires_at IS NOT NULL),
            CONSTRAINT signal_token_operator_is_nobody
                CHECK (kind = 'caller' OR instance_id IS NULL)
        );
CREATE INDEX IF NOT EXISTS idx_signal_token_tenant ON signal_token(tenant_id);
CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            program_json JSONB,
            setup_run_id UUID,
            source_version TEXT,
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            run_id UUID,
            node_id TEXT NOT NULL,
            -- A run's wait (it names its run) or an entry (it names none):
            -- never one without the other.
            is_resume BOOLEAN NOT NULL CHECK (is_resume = (run_id IS NOT NULL)),
            spec_json TEXT NOT NULL,
            -- The connection this signal acts as (`spec.access.id`),
            -- denormalized at register time. NULL for kinds without
            -- one. Inbound provider pushes route account-to-signal
            -- on this column, so the match is one indexed filter
            -- instead of a spec-parsing scan.
            access_id TEXT,
            created_at BIGINT NOT NULL,
            -- Opaque per-kind state persisted at register time and
            -- read back at rehydrate time. Empty for most kinds.
            -- Timer uses it to persist absolute next_fire_at_unix_ms
            -- for After-style schedules so a listener restart
            -- doesn't reset the clock. Future stateful kinds (e.g.
            -- SSE last-event-id, socket reconnect token) use the
            -- same column.
            kind_state JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- The kind_state write fence: a strictly-increasing
            -- per-signal version stamped by the holder's durable
            -- cursor writes. An update whose seq is not above the
            -- stored one is dropped (the newer state stands), and a
            -- register that carries prior state forward loses to any
            -- newer in-flight write the same way. A REAL column, so
            -- the version can never be lost by handling the state
            -- blob (the blob stays purely the kind's own state).
            kind_state_seq BIGINT NOT NULL DEFAULT 0,
            consumer_kind TEXT,
            tags TEXT[] NOT NULL DEFAULT '{}',
            -- The trigger's delivered port values at registration time
            -- (entry signals only). Replayed onto the trigger's ports at
            -- every fire: a trigger's inputs are whatever they were at
            -- trigger setup.
            port_snapshot JSONB,
            consumer_payload TEXT,
            surface_kind TEXT NOT NULL DEFAULT 'task_callback',
            -- A public entry's route pattern under its tenant
            -- (`/<tenant>/chat/{room}`); the dispatcher matches a call
            -- against every pattern of the tenant in Rust, so this is
            -- never an equality lookup key.
            mount_path TEXT,
            -- The HTTP methods the public entry serves, uppercase; empty
            -- = any. Two entries of one tenant may not overlap in both
            -- pattern and method (checked at register time).
            mount_methods TEXT[] NOT NULL DEFAULT '{}',
            -- Whose signal: NULL for the program's shared ones, else the
            -- instance whose copy of a per-instance trigger this is, or whose
            -- run is waiting on it.
            instance_id TEXT,
            -- The trigger whose activation gates this signal (with
            -- `instance_id`, the `trigger_activation` row the fire gate
            -- reads): an entry signal's own trigger, or the trigger that
            -- fired the run a wait belongs to. NULL for a wait of a run
            -- started by hand, which no activation governs.
            activation_trigger TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB,
            -- Whether the signal keeps a connection to the outside open
            -- between fires (its kind's decision for it, from the
            -- listener's `/prepare`): a holder holds it, and the number
            -- of holders is counted from these rows.
            holds BOOLEAN NOT NULL DEFAULT FALSE,
            -- The holder holding its connection now (its replica id): a
            -- claim with no expiry, freed when the holder lets go or when
            -- its dead man's check goes off. NULL while nobody holds it. A
            -- registration that rewrites the row clears it, so whoever
            -- held the old one lets go and the new one comes up fresh.
            held_by TEXT,
            -- Read by nothing: the expiry the previous release's holders
            -- put on their claim, which they still write while an upgrade
            -- rolls out. The release after this one drops it.
            held_until BIGINT,
            -- Which registration this row is: every registration upsert
            -- and restore moves it to a number never used before, so a
            -- wake set for an older one is told apart, and a give-up names
            -- the one it gave up on.
            generation BIGSERIAL NOT NULL,
            -- Why the listener or its holder gave up on it for good,
            -- shown on its node; NULL while it is served. Cleared by a
            -- registration again (`LET_GO`) or by `set_holds`.
            held_failed TEXT,
            -- Why the listener did not let go of it once its trigger
            -- stopped taking work (a hibernation's window ended), so it is
            -- asked again (`crate::listener::note_let_go`); NULL once it
            -- let go. Cleared by a registration again (`LET_GO`).
            let_go_failed TEXT,
            -- What its holder says the connection is doing
            -- (`{"status", "transport"}`), for the node's display, which
            -- another process renders. NULL while nobody holds it.
            serving JSONB,
            -- Its connection (`access_id`) changed or went since this
            -- registration began (`signal_access_stale`): a listener copy
            -- may still keep the old credentials for it, so it is
            -- registered again (`crate::holders::reregister_stale_access`).
            -- Only on a signal no holder holds: a holder hears the change
            -- itself. Cleared by a registration again (`LET_GO`).
            access_stale BOOLEAN NOT NULL DEFAULT FALSE
        );
CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id);
CREATE INDEX IF NOT EXISTS idx_signal_run_id ON signal(run_id);
CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind);
CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_signal_access_stale ON signal(token) WHERE access_stale;
CREATE OR REPLACE FUNCTION signal_access_stale() RETURNS trigger AS $$
            BEGIN
                UPDATE signal SET access_stale = TRUE
                 WHERE access_id = OLD.id::text AND NOT holds AND NOT access_stale;
                IF TG_OP = 'UPDATE' THEN
                    UPDATE signal SET held_failed = NULL
                     WHERE access_id = OLD.id::text AND holds AND held_failed IS NOT NULL;
                END IF;
                PERFORM pg_notify('weft_access_remade', OLD.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS access_grant_stales_signals ON access_grant;
CREATE TRIGGER access_grant_stales_signals
            AFTER UPDATE OF reconnects ON access_grant
            FOR EACH ROW
            WHEN (NEW.reconnects IS DISTINCT FROM OLD.reconnects)
            EXECUTE FUNCTION signal_access_stale();
DROP TRIGGER IF EXISTS access_grant_gone_stales_signals ON access_grant;
CREATE TRIGGER access_grant_gone_stales_signals
            AFTER DELETE ON access_grant
            FOR EACH ROW
            EXECUTE FUNCTION signal_access_stale();
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path, mount_methods) WHERE mount_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_signal_held ON signal(held_by) WHERE holds;
CREATE OR REPLACE FUNCTION signal_held_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_held_signals', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS signal_held_on_insert ON signal;
CREATE TRIGGER signal_held_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NEW.holds)
            EXECUTE FUNCTION signal_held_notify();
DROP TRIGGER IF EXISTS signal_held_on_delete ON signal;
CREATE TRIGGER signal_held_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (OLD.holds)
            EXECUTE FUNCTION signal_held_notify();
DROP TRIGGER IF EXISTS signal_held_on_change ON signal;
CREATE TRIGGER signal_held_on_change
            AFTER UPDATE OF holds, held_failed ON signal
            FOR EACH ROW
            WHEN (NEW.holds IS DISTINCT FROM OLD.holds OR NEW.held_failed IS DISTINCT FROM OLD.held_failed)
            EXECUTE FUNCTION signal_held_notify();
DROP TRIGGER IF EXISTS signal_held_on_release ON signal;
CREATE TRIGGER signal_held_on_release
            AFTER UPDATE OF held_by ON signal
            FOR EACH ROW
            WHEN (NEW.holds AND NEW.held_by IS NULL AND OLD.held_by IS NOT NULL)
            EXECUTE FUNCTION signal_held_notify();
CREATE OR REPLACE FUNCTION routes_notify_tenant() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_routes', OLD.tenant_id);
                ELSE
                    PERFORM pg_notify('weft_routes', NEW.tenant_id);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS signal_routes_on_insert ON signal;
CREATE TRIGGER signal_routes_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NEW.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant();
DROP TRIGGER IF EXISTS signal_routes_on_delete ON signal;
CREATE TRIGGER signal_routes_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (OLD.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant();
DROP TRIGGER IF EXISTS signal_routes_on_change ON signal;
CREATE TRIGGER signal_routes_on_change
            AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json,
                auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id,
                activation_trigger ON signal
            FOR EACH ROW
            WHEN (NEW.surface_kind = 'public_entry' OR OLD.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant();
CREATE OR REPLACE FUNCTION triggers_notify_project() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS signal_triggers_on_insert ON signal;
CREATE TRIGGER signal_triggers_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NOT NEW.is_resume)
            EXECUTE FUNCTION triggers_notify_project();
DROP TRIGGER IF EXISTS signal_triggers_on_delete ON signal;
CREATE TRIGGER signal_triggers_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (NOT OLD.is_resume)
            EXECUTE FUNCTION triggers_notify_project();
DROP TRIGGER IF EXISTS signal_triggers_on_change ON signal;
CREATE TRIGGER signal_triggers_on_change
            AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json,
                auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id,
                activation_trigger, held_by ON signal
            FOR EACH ROW
            WHEN (NOT NEW.is_resume)
            EXECUTE FUNCTION triggers_notify_project();
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_entry_node
             ON signal(project_id, node_id, instance_id) NULLS NOT DISTINCT WHERE is_resume = FALSE;
CREATE INDEX IF NOT EXISTS idx_signal_activation
             ON signal(project_id, activation_trigger, instance_id) WHERE activation_trigger IS NOT NULL;
CREATE TABLE IF NOT EXISTS run_tag (
            seq BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL,
            tag TEXT NOT NULL,
            tagged_at_unix BIGINT NOT NULL,
            UNIQUE (run_id, tag)
        );
CREATE INDEX IF NOT EXISTS idx_run_tag_tag ON run_tag(tag);
CREATE OR REPLACE FUNCTION weft_record_batch(
                p_writer TEXT, p_tenant TEXT, p_lane TEXT, p_now BIGINT, p_delivered_until BIGINT, p_durable BOOLEAN,
                p_ids UUID[], p_epochs INTEGER[], p_first INTEGER[], p_last INTEGER[],
                p_costs BIGINT[], p_skipped INTEGER[], p_wrote BOOLEAN[], p_born JSONB, p_ended JSONB,
                p_row_run INTEGER[], p_row_seq INTEGER[], p_row_events BYTEA[],
                p_resolved_run INTEGER[], p_resolved_token TEXT[],
                p_selections JSONB
            ) RETURNS TABLE (o_run_id UUID, o_fate TEXT, o_project_id UUID, o_tell_end BOOLEAN, o_keep_until BIGINT,
                             o_unwatched BOOLEAN) AS $$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
                v_writer_alive BOOLEAN := TRUE;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                INSERT INTO run_selection (digest, selection)
                    SELECT s->>'digest', s->'selection' FROM jsonb_array_elements(p_selections) AS s
                    ON CONFLICT (digest) DO NOTHING;
                PERFORM 1 FROM run r WHERE r.run_id = ANY(p_ids) ORDER BY r.run_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.run_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.run_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.run_id = v.id;
                WITH born AS (
                    INSERT INTO run (run_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
                                     instance_id, fired_by, source_version, definition_hash, binary_hash, selection,
                                     seed_of, state, owner, epoch, last_seq, started_at, ended_at, outcome, error,
                                     cancel_cause, skipped, wrote_files, cost_micro_usd, keep_for, keep_until,
                                     delivered_until)
                    SELECT v.id, (x.b->>'project_id')::uuid, p_tenant, x.b->>'phase', x.b->>'kind', x.b->>'keeping',
                           (x.b->>'recorded')::boolean, x.b->>'entry_node', x.b->>'instance_id', x.b->>'fired_by',
                           x.b->>'source_version', x.b->>'definition_hash', x.b->>'binary_hash', x.b->>'selection',
                           (x.b->>'seed_of')::uuid,
                           CASE WHEN x.e IS NULL THEN 'running' ELSE 'ended' END,
                           CASE WHEN x.e IS NULL THEN p_writer END,
                           v.epoch, v.last_seq, (x.b->>'started_at')::bigint, (x.e->>'at')::bigint, x.e->>'outcome',
                           x.e->>'error', x.e->'cancel_cause', v.skipped, v.wrote, v.costs, (x.b->>'keep_for')::bigint,
                           (x.e->>'at')::bigint + (x.b->>'keep_for')::bigint,
                           CASE WHEN x.e IS NULL THEN p_delivered_until END
                    FROM unnest(p_ids, p_epochs, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, epoch, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b,
                                                   NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'insert'
                      AND EXISTS (SELECT 1 FROM project p WHERE p.id = (x.b->>'project_id')::uuid AND NOT p.removing)
                    ORDER BY v.id
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs; one
                -- whose project is gone or being removed went nowhere,
                -- and the worker is told which.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.run_id IS NULL AND p.removing THEN 'project_removing'
                                          WHEN v.fate = 'insert' AND r.run_id IS NULL THEN 'refused'
                                          WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                        LEFT JOIN project p ON p.id = (p_born->(v.n::integer - 1)->>'project_id')::uuid;
                END IF;
                UPDATE run r SET
                        last_seq = v.last_seq,
                        cost_micro_usd = r.cost_micro_usd + v.costs,
                        skipped = r.skipped + v.skipped,
                        wrote_files = r.wrote_files OR v.wrote,
                        state = CASE WHEN x.e IS NULL THEN r.state ELSE 'ended' END,
                        owner = CASE WHEN x.e IS NULL THEN r.owner END,
                        ended_at = (x.e->>'at')::bigint,
                        outcome = x.e->>'outcome',
                        error = x.e->>'error',
                        cancel_cause = x.e->'cancel_cause',
                        keep_until = (x.e->>'at')::bigint + r.keep_for,
                        holds_signals = x.e IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.run_id = r.run_id)
                    FROM unnest(p_ids, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'continue' AND r.run_id = v.id;
                INSERT INTO run_log (run_id, seq, events, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (run_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND pf.run_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'project' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (run_id)
                    SELECT r.run_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.run_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.recorded
                    ON CONFLICT (run_id) DO NOTHING;
                INSERT INTO storage_sweep (run_id, tenant_id, enqueued_at_unix)
                    SELECT r.run_id, r.tenant_id, p_now
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.run_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.wrote_files
                    ON CONFLICT (run_id) DO NOTHING;
                IF 'insert' = ANY(v_fates) THEN
                    v_writer_alive := EXISTS (SELECT 1 FROM worker_lease l
                                              WHERE l.replica = p_writer AND l.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT);
                END IF;
                RETURN QUERY
                    SELECT v.id,
                           CASE WHEN v.fate IN ('insert', 'continue') THEN 'accepted' ELSE v.fate END,
                           r.project_id,
                           COALESCE(v.fate IN ('insert', 'continue') AND r.state = 'ended'
                                    AND (r.watch_end OR r.holds_signals), FALSE),
                           CASE WHEN v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                                THEN r.keep_until END,
                           COALESCE(v.fate = 'insert' AND r.state = 'running' AND NOT v_writer_alive, FALSE)
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                    ORDER BY v.n;
            END;
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION weft_record_batch(
                p_writer TEXT, p_tenant TEXT, p_lane TEXT, p_now BIGINT, p_durable BOOLEAN,
                p_ids UUID[], p_epochs INTEGER[], p_first INTEGER[], p_last INTEGER[],
                p_costs BIGINT[], p_skipped INTEGER[], p_wrote BOOLEAN[], p_born JSONB, p_ended JSONB,
                p_row_run INTEGER[], p_row_seq INTEGER[], p_row_events BYTEA[],
                p_resolved_run INTEGER[], p_resolved_token TEXT[],
                p_selections JSONB
            ) RETURNS TABLE (o_run_id UUID, o_fate TEXT, o_project_id UUID, o_tell_end BOOLEAN) AS $$
                SELECT b.o_run_id, CASE WHEN b.o_fate = 'project_removing' THEN 'refused' ELSE b.o_fate END,
                       b.o_project_id, b.o_tell_end
                FROM weft_record_batch(p_writer, p_tenant, p_lane, p_now, NULL::bigint, p_durable,
                                       p_ids, p_epochs, p_first, p_last, p_costs, p_skipped, p_wrote, p_born, p_ended,
                                       p_row_run, p_row_seq, p_row_events, p_resolved_run, p_resolved_token,
                                       p_selections) AS b
            $$ LANGUAGE sql;
