ALTER TABLE run ADD COLUMN busy_since bigint;

ALTER TABLE run ADD COLUMN refusals smallint DEFAULT 0 NOT NULL;

ALTER TABLE run ADD COLUMN unreached smallint DEFAULT 0 NOT NULL;

ALTER TABLE run ADD COLUMN waited_on_workers boolean DEFAULT false NOT NULL;

ALTER TABLE run ADD COLUMN waits boolean DEFAULT false NOT NULL;

ALTER TABLE signal ADD COLUMN access_stale boolean DEFAULT false NOT NULL;

ALTER TABLE signal ADD COLUMN generation bigserial NOT NULL;

ALTER TABLE signal ADD COLUMN held_failed text;

ALTER TABLE signal ADD COLUMN let_go_failed text;

ALTER TABLE run ADD CONSTRAINT run_refusals_not_null NOT NULL refusals;

ALTER TABLE run ADD CONSTRAINT run_unreached_not_null NOT NULL unreached;

ALTER TABLE run ADD CONSTRAINT run_waited_on_workers_not_null NOT NULL waited_on_workers;

ALTER TABLE run ADD CONSTRAINT run_waits_not_null NOT NULL waits;

ALTER TABLE signal ADD CONSTRAINT signal_access_stale_not_null NOT NULL access_stale;

ALTER TABLE signal ADD CONSTRAINT signal_generation_not_null NOT NULL generation;

CREATE OR REPLACE FUNCTION signal_access_stale()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
            $function$
;

CREATE OR REPLACE FUNCTION weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_delivered_until bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_costs bigint[], p_skipped integer[], p_wrote boolean[], p_born jsonb, p_ended jsonb, p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_selections jsonb)
 RETURNS TABLE(o_execution_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean, o_keep_until bigint, o_unwatched boolean)
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
                v_writer_alive BOOLEAN := TRUE;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                INSERT INTO run_selection (digest, selection)
                    SELECT s->>'digest', s->'selection' FROM jsonb_array_elements(p_selections) AS s
                    ON CONFLICT (digest) DO NOTHING;
                PERFORM 1 FROM run r WHERE r.execution_id = ANY(p_ids) ORDER BY r.execution_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.execution_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.execution_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.execution_id = v.id;
                WITH born AS (
                    INSERT INTO run (execution_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
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
                    ON CONFLICT (execution_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs; one
                -- whose project is gone or being removed went nowhere,
                -- and the worker is told which.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.execution_id IS NULL AND p.removing THEN 'project_removing'
                                          WHEN v.fate = 'insert' AND r.execution_id IS NULL THEN 'refused'
                                          WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.execution_id = v.id
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
                        holds_signals = x.e IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.execution_id = r.execution_id)
                    FROM unnest(p_ids, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'continue' AND r.execution_id = v.id;
                INSERT INTO run_log (execution_id, seq, events, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (execution_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND pf.execution_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'execution' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (execution_id)
                    SELECT r.execution_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.recorded
                    ON CONFLICT (execution_id) DO NOTHING;
                INSERT INTO storage_sweep (execution_id, tenant_id, enqueued_at_unix)
                    SELECT r.execution_id, r.tenant_id, p_now
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.wrote_files
                    ON CONFLICT (execution_id) DO NOTHING;
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
                        LEFT JOIN run r ON r.execution_id = v.id
                    ORDER BY v.n;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_costs bigint[], p_skipped integer[], p_wrote boolean[], p_born jsonb, p_ended jsonb, p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_selections jsonb)
 RETURNS TABLE(o_execution_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean)
 LANGUAGE sql
AS $function$
                SELECT b.o_execution_id, CASE WHEN b.o_fate = 'project_removing' THEN 'refused' ELSE b.o_fate END,
                       b.o_project_id, b.o_tell_end
                FROM weft_record_batch(p_writer, p_tenant, p_lane, p_now, NULL::bigint, p_durable,
                                       p_ids, p_epochs, p_first, p_last, p_costs, p_skipped, p_wrote, p_born, p_ended,
                                       p_row_run, p_row_seq, p_row_events, p_resolved_run, p_resolved_token,
                                       p_selections) AS b
            $function$
;

CREATE OR REPLACE FUNCTION worker_lease_workers_back_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF EXISTS (SELECT 1 FROM run r WHERE r.project_id = NEW.project_id AND r.state = 'queued' AND r.unreached >= 3) THEN
                    PERFORM pg_notify('weft_workers_back', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE INDEX idx_signal_access_stale ON signal USING btree (token) WHERE access_stale;

DROP INDEX idx_signal_held;
CREATE INDEX idx_signal_held ON signal USING btree (held_by) WHERE holds;

CREATE TRIGGER access_grant_gone_stales_signals AFTER DELETE ON access_grant FOR EACH ROW EXECUTE FUNCTION signal_access_stale();

CREATE TRIGGER access_grant_stales_signals AFTER UPDATE OF reconnects ON access_grant FOR EACH ROW WHEN ((new.reconnects IS DISTINCT FROM old.reconnects)) EXECUTE FUNCTION signal_access_stale();

DROP TRIGGER signal_held_on_change ON signal;
CREATE TRIGGER signal_held_on_change AFTER UPDATE OF holds, held_failed ON signal FOR EACH ROW WHEN (((new.holds IS DISTINCT FROM old.holds) OR (new.held_failed IS DISTINCT FROM old.held_failed))) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER signal_held_on_release AFTER UPDATE OF held_by ON signal FOR EACH ROW WHEN ((new.holds AND (new.held_by IS NULL) AND (old.held_by IS NOT NULL))) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER worker_lease_workers_back_on_insert AFTER INSERT ON worker_lease FOR EACH ROW EXECUTE FUNCTION worker_lease_workers_back_notify();

CREATE TRIGGER worker_lease_workers_back_on_renewal AFTER UPDATE OF leased_until_unix ON worker_lease FOR EACH ROW WHEN ((old.leased_until_unix < (EXTRACT(epoch FROM now()))::bigint)) EXECUTE FUNCTION worker_lease_workers_back_notify();
