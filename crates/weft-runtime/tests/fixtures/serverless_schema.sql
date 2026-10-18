-- The last schema of the `serverless` generation, as its own migrations left it,
-- with the ledger rows that name the generation: what `weft-runtime carry` reads from.
-- Written by pg_dump; never edited.
--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: access_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.access_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP <> 'INSERT' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF OLD.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || OLD.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', OLD.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', OLD.project_id::text);
                    END IF;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF NEW.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || NEW.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', NEW.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: infra_command_cancel_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.infra_command_cancel_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_command', 'cancel:' || NEW.id::text);
                RETURN NULL;
            END;
            $$;


--
-- Name: infra_command_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.infra_command_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NEW.drain_by_unix IS NULL) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: infra_event_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.infra_event_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.project_id::text || ' ' || NEW.id::text);
                RETURN NULL;
            END;
            $$;


--
-- Name: infra_node_status_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.infra_node_status_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_infra_status', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_status', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: install_domain_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.install_domain_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_install_domains', '');
                RETURN NULL;
            END;
            $$;


--
-- Name: parked_fire_drop_with_signal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parked_fire_drop_with_signal() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                DELETE FROM parked_fire WHERE token = OLD.token AND execution_id IS NULL;
                RETURN NULL;
            END;
            $$;


--
-- Name: parked_fire_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parked_fire_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            DECLARE
                v_project UUID;
            BEGIN
                IF NEW.execution_id IS NULL THEN
                    PERFORM weft_announce('weft_parked_fire', (SELECT s.project_id FROM signal s WHERE s.token = NEW.token)::text);
                ELSE
                    SELECT r.project_id INTO v_project FROM run r WHERE r.execution_id = NEW.execution_id;
                    PERFORM weft_announce('weft_parked_fire', v_project::text || ' ' || NEW.execution_id::text);
                    PERFORM weft_tell(v_project, NEW.execution_id, 'answer');
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: project_declared_infra_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.project_declared_infra_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_status', NEW.id::text);
                RETURN NULL;
            END;
            $$;


--
-- Name: project_worker_settings_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.project_worker_settings_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_worker_settings', OLD.id::text);
                RETURN NULL;
            END;
            $$;


--
-- Name: routes_notify_tenant(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.routes_notify_tenant() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_routes', OLD.tenant_id);
                ELSE
                    PERFORM pg_notify('weft_routes', NEW.tenant_id);
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: signal_access_stale(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.signal_access_stale() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
            $$;


--
-- Name: signal_held_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.signal_held_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_held_signals', '');
                RETURN NULL;
            END;
            $$;


--
-- Name: storage_sweep_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.storage_sweep_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM weft_announce('weft_storage_sweep', '');
                RETURN NULL;
            END;
            $$;


--
-- Name: task_ready_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.task_ready_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM weft_announce('weft_task_ready', '');
                RETURN NULL;
            END;
            $$;


--
-- Name: trigger_activation_routes_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trigger_activation_routes_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
            $$;


--
-- Name: trigger_activation_triggers_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trigger_activation_triggers_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: triggers_notify_project(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.triggers_notify_project() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$;


--
-- Name: weft_announce(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_announce(p_channel text, p_payload text) RETURNS void
    LANGUAGE sql
    AS $$
            INSERT INTO weft_announcement (channel, payload) VALUES (p_channel, p_payload)
            $$;


--
-- Name: weft_door_tick(text, uuid, text, text, bigint, jsonb, bigint, text[], bigint[], text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_door_tick(p_replica text, p_project uuid, p_tenant text, p_binary_hash text, p_leased_until bigint, p_in_flight jsonb, p_window bigint, p_keys text[], p_hits bigint[], p_tokens text[]) RETURNS jsonb
    LANGUAGE sql
    AS $$
                SELECT jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                AND split_part(key, ':', 2) = ANY(p_tokens)
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', t.tick->'copies')
                FROM (SELECT weft_door_tick(p_replica, p_project, p_tenant, p_binary_hash, p_leased_until,
                                            p_in_flight, p_window, p_keys, p_hits, FALSE) AS tick) t
            $$;


--
-- Name: weft_door_tick(text, uuid, text, text, bigint, jsonb, bigint, text[], bigint[], boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_door_tick(p_replica text, p_project uuid, p_tenant text, p_binary_hash text, p_leased_until bigint, p_in_flight jsonb, p_window bigint, p_keys text[], p_hits bigint[], p_shortens boolean) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, in_flight, alive_since)
                    VALUES (p_replica, p_project, p_tenant, p_binary_hash, p_leased_until, p_in_flight,
                            EXTRACT(EPOCH FROM NOW())::BIGINT)
                    ON CONFLICT (replica) DO UPDATE
                        SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight,
                            alive_since = CASE WHEN worker_lease.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT
                                               THEN worker_lease.alive_since ELSE EXCLUDED.alive_since END
                    WHERE worker_lease.project_id = EXCLUDED.project_id;
                INSERT INTO door_count (key, window_start, replica, project_id, hits)
                    SELECT k, p_window, p_replica, p_project, h FROM unnest(p_keys, p_hits) AS t(k, h)
                    ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = EXCLUDED.hits
                    WHERE door_count.project_id = EXCLUDED.project_id;
                -- This tick's lease ends before the one the worker vouched
                -- for last: the look at lapsed leases, booked for the
                -- longer one, is rung again.
                -- SYNC: 'weft_lease_alive' <-> LEASE_ALIVE_CHANNEL
                IF p_shortens THEN
                    PERFORM pg_notify('weft_lease_alive', p_replica);
                END IF;
                RETURN jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                -- SYNC: '-' <-> crates/weft-engine/src/door/limits.rs (invalid_tokens_key)
                                AND split_part(key, ':', 2) <> '-' 
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', (SELECT COUNT(*) FROM worker_lease l
                               WHERE l.project_id = p_project
                                 AND l.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT));
            END;
            $$;


--
-- Name: weft_enqueue_dedup(uuid, text, uuid, text, uuid, text, jsonb, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_enqueue_dedup(p_id uuid, p_kind text, p_project uuid, p_dedup text, p_execution uuid, p_tenant text, p_payload jsonb, p_now bigint, OUT task_id uuid, OUT inserted boolean) RETURNS record
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM weft_lock_dedup(p_tenant, p_kind, p_dedup);
                SELECT t.id INTO task_id FROM task t
                    WHERE t.tenant_id = p_tenant AND t.kind = p_kind AND t.dedup_key = p_dedup
                      AND t.status IN ('pending', 'claimed')
                    LIMIT 1;
                IF FOUND THEN
                    inserted := FALSE;
                    RETURN;
                END IF;
                INSERT INTO task (id, kind, status, project_id, dedup_key, execution_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_execution, p_tenant, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $$;


--
-- Name: weft_flush_announcements(integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_flush_announcements(p_limit integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
            DECLARE
                v_channels TEXT[];
                v_payloads TEXT[];
            BEGIN
                WITH taken AS (
                    DELETE FROM weft_announcement a
                    WHERE a.ctid IN (SELECT ctid FROM weft_announcement FOR UPDATE SKIP LOCKED LIMIT p_limit)
                    RETURNING channel, payload)
                SELECT array_agg(channel), array_agg(payload) INTO v_channels, v_payloads FROM taken;
                IF v_channels IS NULL THEN
                    RETURN 0;
                END IF;
                PERFORM pg_notify(d.channel, d.payload)
                    FROM (SELECT DISTINCT u.channel, u.payload FROM unnest(v_channels, v_payloads) AS u(channel, payload)) d;
                RETURN array_length(v_channels, 1);
            END;
            $$;


--
-- Name: weft_lock_dedup(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_lock_dedup(p_tenant text, p_kind text, p_dedup text) RETURNS void
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended(p_tenant || '|' || p_kind || '|' || p_dedup, 0));
            END;
            $$;


--
-- Name: weft_news_refuse(bigint[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_news_refuse(p_seqs bigint[]) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
            DECLARE
                v_marked BIGINT;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                UPDATE weft_news SET refused_at = clock_timestamp() WHERE seq = ANY(p_seqs);
                GET DIAGNOSTICS v_marked = ROW_COUNT;
                RETURN v_marked;
            END;
            $$;


--
-- Name: weft_news_refused(bigint, bigint[], text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_news_refused(p_limit bigint, p_skip bigint[], p_callee text) RETURNS TABLE(seq bigint, project_id uuid, execution_id uuid, kind text)
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                IF NOT EXISTS (SELECT 1 FROM weft_news n WHERE n.refused_at IS NOT NULL) THEN
                    DELETE FROM unanswered_call u WHERE u.callee = p_callee;
                    RETURN;
                END IF;
                RETURN QUERY SELECT n.seq, n.project_id, n.execution_id, n.kind FROM weft_news n
                    WHERE n.refused_at IS NOT NULL AND n.seq <> ALL(p_skip)
                    ORDER BY n.refused_at, n.seq LIMIT p_limit;
            END;
            $$;


--
-- Name: weft_prune_finished_tasks(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_prune_finished_tasks(p_before bigint) RETURNS integer
    LANGUAGE sql
    AS $$
            WITH gone AS (
                DELETE FROM task WHERE id IN (
                    SELECT id FROM task
                    WHERE status IN ('complete', 'failed') AND completed_at_unix < p_before
                    LIMIT 500 FOR UPDATE SKIP LOCKED)
                RETURNING 1)
            SELECT count(*)::INTEGER FROM gone
            $$;


--
-- Name: weft_rate_hit(text, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_rate_hit(p_key text, p_window_start bigint) RETURNS integer
    LANGUAGE sql
    AS $$
            DELETE FROM entry_rate WHERE window_start < p_window_start - 60;
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $$;


--
-- Name: weft_record_batch(text, text, text, bigint, boolean, uuid[], integer[], integer[], integer[], bigint[], integer[], boolean[], jsonb, jsonb, integer[], integer[], bytea[], integer[], text[], jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_costs bigint[], p_skipped integer[], p_wrote boolean[], p_born jsonb, p_ended jsonb, p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_selections jsonb) RETURNS TABLE(o_execution_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean)
    LANGUAGE sql
    AS $$
                SELECT b.o_execution_id, CASE WHEN b.o_fate = 'project_removing' THEN 'refused' ELSE b.o_fate END,
                       b.o_project_id, b.o_tell_end
                FROM weft_record_batch(p_writer, p_tenant, p_lane, p_now, NULL::bigint, p_durable,
                                       p_ids, p_epochs, p_first, p_last, p_costs, p_skipped, p_wrote, p_born, p_ended,
                                       p_row_run, p_row_seq, p_row_events, p_resolved_run, p_resolved_token,
                                       p_selections) AS b
            $$;


--
-- Name: weft_record_batch(text, text, text, bigint, bigint, boolean, uuid[], integer[], integer[], integer[], bigint[], integer[], boolean[], jsonb, jsonb, integer[], integer[], bytea[], integer[], text[], jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_record_batch(p_writer text, p_tenant text, p_lane text, p_now bigint, p_delivered_until bigint, p_durable boolean, p_ids uuid[], p_epochs integer[], p_first integer[], p_last integer[], p_costs bigint[], p_skipped integer[], p_wrote boolean[], p_born jsonb, p_ended jsonb, p_row_run integer[], p_row_seq integer[], p_row_events bytea[], p_resolved_run integer[], p_resolved_token text[], p_selections jsonb) RETURNS TABLE(o_execution_id uuid, o_fate text, o_project_id uuid, o_tell_end boolean, o_keep_until bigint, o_unwatched boolean)
    LANGUAGE plpgsql
    AS $$
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
            $$;


--
-- Name: weft_tell(uuid, uuid, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.weft_tell(p_project uuid, p_execution uuid, p_kind text) RETURNS void
    LANGUAGE sql
    AS $$
            INSERT INTO weft_news (project_id, execution_id, kind) VALUES (p_project, p_execution, p_kind)
            $$;


--
-- Name: worker_lease_alive_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.worker_lease_alive_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                PERFORM pg_notify('weft_lease_alive', NEW.replica);
                RETURN NULL;
            END;
            $$;


--
-- Name: worker_lease_workers_back_notify(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.worker_lease_workers_back_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
            BEGIN
                IF EXISTS (SELECT 1 FROM run r WHERE r.project_id = NEW.project_id AND r.state = 'queued' AND r.unreached >= 3) THEN
                    PERFORM pg_notify('weft_workers_back', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: access_connect; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_connect (
    state text NOT NULL,
    tenant_id text NOT NULL,
    service text NOT NULL,
    registration_sealed text NOT NULL,
    project_id uuid,
    spec_json jsonb NOT NULL,
    scopes jsonb DEFAULT '[]'::jsonb NOT NULL,
    pkce_verifier text,
    door text DEFAULT 'own'::text NOT NULL,
    upgrade_grant_id uuid,
    instance_id text,
    redirect_uri text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: access_connect_result; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_connect_result (
    state text NOT NULL,
    tenant_id text NOT NULL,
    result_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: access_grant; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_grant (
    id uuid NOT NULL,
    tenant_id text NOT NULL,
    service text NOT NULL,
    registration_sealed text,
    client_id text,
    project_id uuid,
    spec_json jsonb NOT NULL,
    values_sealed text NOT NULL,
    value_names jsonb DEFAULT '[]'::jsonb NOT NULL,
    granted_scopes jsonb DEFAULT '[]'::jsonb NOT NULL,
    permissions_verified boolean DEFAULT false NOT NULL,
    owner text DEFAULT 'their-own'::text NOT NULL,
    door text DEFAULT 'own'::text NOT NULL,
    label text,
    identity text,
    provider_account text,
    events_recipe_hash text,
    published_by_node text,
    instance_id text,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    reconnects bigint DEFAULT 0 NOT NULL,
    CONSTRAINT access_grant_instance_has_project CHECK (((instance_id IS NULL) OR (project_id IS NOT NULL)))
);


--
-- Name: access_picker; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_picker (
    state text NOT NULL,
    tenant_id text NOT NULL,
    access_id uuid NOT NULL,
    service text NOT NULL,
    script text NOT NULL,
    code text NOT NULL,
    mime_types jsonb DEFAULT '[]'::jsonb NOT NULL,
    grants jsonb DEFAULT '[]'::jsonb NOT NULL,
    project_id uuid,
    instance_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: alarm; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alarm (
    name text NOT NULL,
    key text NOT NULL,
    at_unix_ms bigint NOT NULL,
    set_for_unix_ms bigint NOT NULL,
    role text NOT NULL,
    path text NOT NULL,
    body jsonb NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    answered integer DEFAULT 0 NOT NULL
);


--
-- Name: asset_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.asset_reference (
    key text NOT NULL,
    project_id text NOT NULL,
    tenant_id text NOT NULL
);


--
-- Name: door_count; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.door_count (
    key text NOT NULL,
    window_start bigint NOT NULL,
    replica text NOT NULL,
    project_id uuid NOT NULL,
    hits bigint NOT NULL
);


--
-- Name: entry_rate; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.entry_rate (
    key text NOT NULL,
    window_start bigint NOT NULL,
    hits integer NOT NULL
);


--
-- Name: execution_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_tag (
    seq bigint NOT NULL,
    execution_id uuid NOT NULL,
    tag text NOT NULL,
    tagged_at_unix bigint NOT NULL
);


--
-- Name: execution_tag_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.execution_tag_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: execution_tag_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.execution_tag_seq_seq OWNED BY public.execution_tag.seq;


--
-- Name: image_build; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.image_build (
    image_ref text NOT NULL,
    project_id uuid NOT NULL,
    tenant_id text NOT NULL,
    build_name text NOT NULL,
    lane integer,
    status text NOT NULL,
    reason text,
    held_until bigint NOT NULL,
    started_at bigint NOT NULL,
    finished_at bigint,
    log_url text,
    builder_id text,
    failing_since bigint,
    CONSTRAINT image_build_status_check CHECK ((status = ANY (ARRAY['running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: image_claim; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.image_claim (
    claim_id uuid NOT NULL,
    holder_until bigint NOT NULL,
    image_ref text NOT NULL,
    project_id uuid
);


--
-- Name: image_use; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.image_use (
    image_ref text NOT NULL,
    project_id uuid NOT NULL,
    running_since bigint NOT NULL
);


--
-- Name: infra_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_event (
    id bigint NOT NULL,
    tenant_id text NOT NULL,
    project_id uuid NOT NULL,
    node_id text,
    instance_id text,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    at_unix bigint NOT NULL
);


--
-- Name: infra_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.infra_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: infra_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.infra_event_id_seq OWNED BY public.infra_event.id;


--
-- Name: infra_health_look; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_health_look (
    asked_at_unix bigint,
    failures integer DEFAULT 0 NOT NULL,
    project_id uuid NOT NULL,
    retry_at_unix bigint
);


--
-- Name: infra_health_protocol; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_health_protocol (
    acted_json jsonb,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    gave_up_at_unix bigint,
    gave_up_seen jsonb,
    last_error text,
    next_retry_at_unix bigint,
    project_id uuid NOT NULL,
    protocol text NOT NULL
);


--
-- Name: infra_lifecycle_command; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_lifecycle_command (
    id bigint NOT NULL,
    tenant_id text NOT NULL,
    project_id uuid NOT NULL,
    node_id text,
    verb text NOT NULL,
    spec_json jsonb,
    issued_by_replica text NOT NULL,
    issued_at_unix bigint NOT NULL,
    claimed_by_replica text,
    claimed_at_unix bigint,
    completed_at_unix bigint,
    outcome text,
    outcome_message text,
    force boolean DEFAULT false NOT NULL,
    cancel_requested boolean DEFAULT false NOT NULL,
    instance_id text,
    every_copy boolean DEFAULT false NOT NULL,
    asked_by uuid,
    drain_by_unix bigint,
    runs integer DEFAULT 0 NOT NULL
);


--
-- Name: infra_lifecycle_command_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.infra_lifecycle_command_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: infra_lifecycle_command_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.infra_lifecycle_command_id_seq OWNED BY public.infra_lifecycle_command.id;


--
-- Name: infra_node; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_node (
    project_id uuid NOT NULL,
    node_id text NOT NULL,
    copy_id text DEFAULT ''::text NOT NULL,
    status text NOT NULL,
    failure_stage text,
    failure_message text,
    applied_spec_hash text,
    applied_at_unix bigint,
    endpoints_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    public_paths_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    keep_disks_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    units_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    instance_id text,
    doors_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    install_endpoints_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    notes_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    provisioning_since_unix bigint,
    waiting_on text,
    baked_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: infra_owner; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.infra_owner (
    project_id uuid NOT NULL,
    supervisor_replica text NOT NULL,
    tenant_id text NOT NULL,
    leased_until_unix bigint NOT NULL
);


--
-- Name: install_domain; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.install_domain (
    name text NOT NULL,
    serves text NOT NULL,
    project_id uuid,
    upstream text,
    added_unix bigint NOT NULL,
    CONSTRAINT install_domain_check CHECK (((serves = 'install'::text) = (project_id IS NULL))),
    CONSTRAINT install_domain_check1 CHECK (((serves = 'frontend'::text) = (upstream IS NOT NULL))),
    CONSTRAINT install_domain_serves_check CHECK ((serves = ANY (ARRAY['install'::text, 'frontend'::text, 'api'::text])))
);


--
-- Name: install_domain_door; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.install_domain_door (
    refusals integer NOT NULL,
    refused text,
    served text[] NOT NULL,
    singleton boolean DEFAULT true NOT NULL,
    served_at_ms bigint NOT NULL,
    CONSTRAINT install_domain_door_singleton_check CHECK (singleton)
);


--
-- Name: install_pick; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.install_pick (
    tenant_id text NOT NULL,
    project_id uuid NOT NULL,
    step text NOT NULL,
    field text NOT NULL,
    grant_id uuid NOT NULL,
    set_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: instance_value; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.instance_value (
    tenant_id text NOT NULL,
    project_id uuid NOT NULL,
    instance_id text NOT NULL,
    step text NOT NULL,
    field text NOT NULL,
    value jsonb NOT NULL,
    grant_id uuid,
    set_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: parked_fire; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.parked_fire (
    token text NOT NULL,
    seq bigint NOT NULL,
    fire_id uuid NOT NULL,
    payload jsonb NOT NULL,
    caller text,
    attempts integer DEFAULT 0 NOT NULL,
    not_before bigint NOT NULL,
    instance_gap jsonb,
    is_resume boolean NOT NULL,
    execution_id uuid,
    skipped boolean DEFAULT false NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    waits_for text
);


--
-- Name: parked_fire_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.parked_fire ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.parked_fire_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: project; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project (
    id uuid NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    status text NOT NULL,
    project_json text NOT NULL,
    updated_at bigint NOT NULL,
    running_binary_hash text,
    running_definition_hash text,
    running_infra_hash text,
    running_source jsonb,
    tenant_id text NOT NULL,
    has_infra boolean DEFAULT false NOT NULL,
    infra_image_tags_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    worker_settings_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    health_protocols_json jsonb,
    transition text DEFAULT 'none'::text NOT NULL,
    transition_heartbeat_unix bigint DEFAULT 0 NOT NULL,
    head_version text,
    head_run uuid,
    api_port integer,
    api_address jsonb,
    build_asks bigint DEFAULT 0 NOT NULL,
    registered_ask bigint DEFAULT 0 NOT NULL,
    removing boolean DEFAULT false NOT NULL,
    resync jsonb
);


--
-- Name: project_code; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project_code (
    project_id uuid NOT NULL,
    binary_hash text NOT NULL,
    implementations jsonb NOT NULL
);


--
-- Name: project_definition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project_definition (
    project_id uuid NOT NULL,
    definition_hash text NOT NULL,
    project_json text NOT NULL,
    recorded_at_unix bigint NOT NULL
);


--
-- Name: project_frontend; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project_frontend (
    project_id uuid NOT NULL,
    name text NOT NULL,
    host text NOT NULL,
    repo text,
    service text,
    url text,
    token_id uuid,
    added_unix bigint NOT NULL,
    repo_id bigint,
    pending_token_ids uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    CONSTRAINT project_frontend_check CHECK (((host = 'cloud_run'::text) = (repo IS NOT NULL))),
    CONSTRAINT project_frontend_check1 CHECK (((repo IS NULL) = (repo_id IS NULL))),
    CONSTRAINT project_frontend_host_check CHECK ((host = ANY (ARRAY['cloud_run'::text, 'external'::text])))
);


--
-- Name: project_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project_version (
    id text NOT NULL,
    project_id uuid NOT NULL,
    parent_id text,
    manifest jsonb NOT NULL,
    label text,
    created_at bigint NOT NULL,
    seq bigint NOT NULL
);


--
-- Name: project_version_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.project_version_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_version_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.project_version_seq_seq OWNED BY public.project_version.seq;


--
-- Name: public_file_link; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.public_file_link (
    token text NOT NULL,
    key text NOT NULL,
    mime_type text NOT NULL,
    filename text NOT NULL,
    fetch_url text NOT NULL,
    expires_at_unix bigint NOT NULL
);


--
-- Name: role_loop_due; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.role_loop_due (
    role text NOT NULL,
    loop_name text NOT NULL,
    due_ms bigint,
    written_ms bigint NOT NULL,
    failure_written boolean DEFAULT false NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    seq bigint DEFAULT 0 NOT NULL,
    set_again integer DEFAULT 0 NOT NULL
);


--
-- Name: run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run (
    attempts integer DEFAULT 1 NOT NULL,
    binary_hash text,
    cancel_cause jsonb,
    cancel_requested jsonb,
    cost_micro_usd bigint DEFAULT 0 NOT NULL,
    definition_hash text,
    delivered_until bigint,
    ended_at bigint,
    entry_node text,
    epoch integer DEFAULT 1 NOT NULL,
    error text,
    example text,
    execution_id uuid NOT NULL,
    fired_by text,
    holds_signals boolean DEFAULT false NOT NULL,
    instance_id text,
    keep_for bigint,
    keep_until bigint,
    keeping text NOT NULL,
    kind text NOT NULL,
    last_seq integer NOT NULL,
    outcome text,
    owner text,
    phase text NOT NULL,
    project_id uuid NOT NULL,
    recorded boolean NOT NULL,
    seed_of uuid,
    selection text,
    skipped integer DEFAULT 0 NOT NULL,
    source_version text,
    spec jsonb,
    stale text[] DEFAULT '{}'::text[] NOT NULL,
    started_at bigint NOT NULL,
    state text NOT NULL,
    tenant_id text NOT NULL,
    watch_end boolean DEFAULT false NOT NULL,
    wrote_files boolean DEFAULT false NOT NULL,
    busy_since bigint,
    refusals smallint DEFAULT 0 NOT NULL,
    unreached smallint DEFAULT 0 NOT NULL,
    waited_on_workers boolean DEFAULT false NOT NULL,
    waits boolean DEFAULT false NOT NULL
);


--
-- Name: run_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_log (
    events bytea NOT NULL,
    execution_id uuid NOT NULL,
    seq integer NOT NULL,
    writer text NOT NULL,
    written_at bigint NOT NULL
);


--
-- Name: run_search; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_search (
    execution_id uuid NOT NULL,
    nodes text[] NOT NULL,
    words tsvector NOT NULL
);


--
-- Name: run_search_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_search_queue (
    execution_id uuid NOT NULL
);


--
-- Name: run_selection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_selection (
    digest text NOT NULL,
    selection jsonb NOT NULL
);


--
-- Name: runtime_file; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_file (
    key text NOT NULL,
    tenant_id text NOT NULL,
    mime_type text NOT NULL,
    filename text NOT NULL,
    size_bytes bigint NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    keep boolean DEFAULT false NOT NULL,
    expires_at_unix bigint,
    keep_ttl_secs bigint,
    created_at_unix bigint NOT NULL,
    upload_id text,
    part_size bigint,
    declared_size bigint,
    reserved_bytes bigint DEFAULT 0 NOT NULL,
    progressed_at_unix bigint DEFAULT 0 NOT NULL,
    identity text,
    replaces text,
    version bigint DEFAULT 1 NOT NULL,
    last_replacement text,
    reap_error text
);


--
-- Name: runtime_file_part; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_file_part (
    key text NOT NULL,
    part_number integer NOT NULL,
    size_bytes bigint NOT NULL,
    etag text
);


--
-- Name: service_events_recipe; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.service_events_recipe (
    service text NOT NULL,
    recipe_hash text NOT NULL,
    events_json jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: signal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.signal (
    token text NOT NULL,
    program_json jsonb,
    setup_execution_id uuid,
    source_version text,
    tenant_id text NOT NULL,
    project_id uuid NOT NULL,
    execution_id uuid,
    node_id text NOT NULL,
    is_resume boolean NOT NULL,
    spec_json text NOT NULL,
    access_id text,
    created_at bigint NOT NULL,
    kind_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    kind_state_seq bigint DEFAULT 0 NOT NULL,
    consumer_kind text,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    port_snapshot jsonb,
    consumer_payload text,
    surface_kind text DEFAULT 'task_callback'::text NOT NULL,
    mount_path text,
    mount_methods text[] DEFAULT '{}'::text[] NOT NULL,
    instance_id text,
    activation_trigger text,
    auth_kind text DEFAULT 'none'::text NOT NULL,
    auth_config jsonb,
    held_by text,
    held_until bigint,
    holds boolean DEFAULT false NOT NULL,
    serving jsonb,
    access_stale boolean DEFAULT false NOT NULL,
    generation bigint NOT NULL,
    held_failed text,
    let_go_failed text,
    CONSTRAINT signal_check CHECK ((is_resume = (execution_id IS NOT NULL)))
);


--
-- Name: signal_generation_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.signal_generation_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: signal_generation_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.signal_generation_seq OWNED BY public.signal.generation;


--
-- Name: signal_subscription; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.signal_subscription (
    id text NOT NULL,
    tenant_id text NOT NULL,
    service text NOT NULL,
    topic text NOT NULL,
    access_id uuid NOT NULL,
    signal_token text NOT NULL,
    token_sealed text NOT NULL,
    captures_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    expires_at timestamp with time zone,
    project_id uuid,
    instance_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: signal_token; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.signal_token (
    id uuid NOT NULL,
    token_hash text NOT NULL,
    recognizer text NOT NULL,
    tenant_id text NOT NULL,
    name text,
    allowed_projects uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    allowed_tags text[] DEFAULT '{}'::text[] NOT NULL,
    allowed_displays text[] DEFAULT '{}'::text[] NOT NULL,
    all_displays boolean DEFAULT false NOT NULL,
    created_at bigint NOT NULL,
    instance_id text,
    expires_at bigint,
    kind text DEFAULT 'caller'::text NOT NULL,
    CONSTRAINT signal_token_instance_expires CHECK (((instance_id IS NULL) OR (expires_at IS NOT NULL))),
    CONSTRAINT signal_token_instance_has_one_project CHECK (((instance_id IS NULL) OR (cardinality(allowed_projects) = 1))),
    CONSTRAINT signal_token_kind_check CHECK ((kind = ANY (ARRAY['caller'::text, 'operator'::text]))),
    CONSTRAINT signal_token_operator_is_nobody CHECK (((kind = 'caller'::text) OR (instance_id IS NULL)))
);


--
-- Name: storage_sweep; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.storage_sweep (
    execution_id uuid NOT NULL,
    tenant_id text NOT NULL,
    enqueued_at_unix bigint NOT NULL
);


--
-- Name: task; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task (
    id uuid NOT NULL,
    kind text NOT NULL,
    status text NOT NULL,
    project_id uuid,
    dedup_key text,
    execution_id uuid,
    tenant_id text NOT NULL,
    payload jsonb NOT NULL,
    claimed_by text,
    claimed_until_unix bigint,
    attempts integer DEFAULT 0 NOT NULL,
    result jsonb,
    error text,
    created_at_unix bigint NOT NULL,
    completed_at_unix bigint,
    claim_rests_on uuid
);


--
-- Name: trigger_activation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trigger_activation (
    project_id uuid NOT NULL,
    trigger text NOT NULL,
    instance_id text,
    status text NOT NULL,
    accepting_fires boolean NOT NULL,
    fires_visible_to_consumers boolean NOT NULL,
    fires_deadline_unix bigint,
    drain_deadline_unix bigint,
    activating_execution_id uuid,
    heartbeat_unix bigint DEFAULT 0 NOT NULL,
    activation_program jsonb,
    activation_version text,
    updated_at bigint NOT NULL,
    went_down_at_unix bigint,
    went_down_with text
);


--
-- Name: trigger_bake; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trigger_bake (
    project_id uuid NOT NULL,
    instance_id text,
    program_hash text NOT NULL,
    bake_json text NOT NULL
);


--
-- Name: trigger_setup; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trigger_setup (
    project_id uuid NOT NULL,
    execution_id uuid NOT NULL
);


--
-- Name: unanswered_alert; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.unanswered_alert (
    after_ms bigint NOT NULL,
    one boolean DEFAULT true NOT NULL,
    CONSTRAINT unanswered_alert_one_check CHECK (one)
);


--
-- Name: unanswered_call; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.unanswered_call (
    callee text NOT NULL,
    error text NOT NULL,
    since_ms bigint NOT NULL,
    last_ms bigint NOT NULL,
    alerted_ms bigint,
    label text
);


--
-- Name: version_build; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.version_build (
    asked_at bigint NOT NULL,
    ended_at bigint,
    id uuid NOT NULL,
    images text[] NOT NULL,
    manifest jsonb NOT NULL,
    program jsonb NOT NULL,
    project_id uuid NOT NULL,
    project_name text NOT NULL,
    reason text,
    registered jsonb,
    state text NOT NULL,
    tenant_id text NOT NULL,
    waits_on jsonb NOT NULL,
    ask bigint NOT NULL,
    CONSTRAINT version_build_state_check CHECK ((state = ANY (ARRAY['waiting'::text, 'registered'::text, 'failed'::text, 'cancelled'::text, 'superseded'::text])))
);


--
-- Name: version_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.version_runs (
    lane text NOT NULL,
    last_run uuid NOT NULL,
    project_id uuid NOT NULL,
    runs bigint NOT NULL,
    source_version text NOT NULL
);


--
-- Name: weft_announcement; Type: TABLE; Schema: public; Owner: -
--

CREATE UNLOGGED TABLE public.weft_announcement (
    channel text NOT NULL,
    payload text NOT NULL
);


--
-- Name: weft_migration; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.weft_migration (
    group_name text NOT NULL,
    id text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: weft_news; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.weft_news (
    execution_id uuid NOT NULL,
    kind text NOT NULL,
    project_id uuid NOT NULL,
    seq bigint NOT NULL,
    refused_at timestamp with time zone,
    CONSTRAINT weft_news_kind_check CHECK ((kind = ANY (ARRAY['answer'::text, 'cancel'::text])))
);


--
-- Name: weft_news_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.weft_news ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.weft_news_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: weft_schema_stamp; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.weft_schema_stamp (
    group_name text NOT NULL,
    fingerprint text NOT NULL
);


--
-- Name: worker_lease; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.worker_lease (
    replica text NOT NULL,
    project_id uuid NOT NULL,
    tenant_id text NOT NULL,
    leased_until_unix bigint NOT NULL,
    binary_hash text,
    in_flight jsonb DEFAULT '{}'::jsonb NOT NULL,
    alive_since bigint DEFAULT 0 NOT NULL
);


--
-- Name: execution_tag seq; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_tag ALTER COLUMN seq SET DEFAULT nextval('public.execution_tag_seq_seq'::regclass);


--
-- Name: infra_event id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_event ALTER COLUMN id SET DEFAULT nextval('public.infra_event_id_seq'::regclass);


--
-- Name: infra_lifecycle_command id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_lifecycle_command ALTER COLUMN id SET DEFAULT nextval('public.infra_lifecycle_command_id_seq'::regclass);


--
-- Name: project_version seq; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_version ALTER COLUMN seq SET DEFAULT nextval('public.project_version_seq_seq'::regclass);


--
-- Name: signal generation; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signal ALTER COLUMN generation SET DEFAULT nextval('public.signal_generation_seq'::regclass);


--
-- Name: access_connect access_connect_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_connect
    ADD CONSTRAINT access_connect_pkey PRIMARY KEY (state);


--
-- Name: access_connect_result access_connect_result_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_connect_result
    ADD CONSTRAINT access_connect_result_pkey PRIMARY KEY (state);


--
-- Name: access_grant access_grant_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_grant
    ADD CONSTRAINT access_grant_pkey PRIMARY KEY (id);


--
-- Name: access_picker access_picker_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_picker
    ADD CONSTRAINT access_picker_pkey PRIMARY KEY (state);


--
-- Name: alarm alarm_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alarm
    ADD CONSTRAINT alarm_pkey PRIMARY KEY (name);


--
-- Name: asset_reference asset_reference_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asset_reference
    ADD CONSTRAINT asset_reference_pkey PRIMARY KEY (tenant_id, project_id, key);


--
-- Name: door_count door_count_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.door_count
    ADD CONSTRAINT door_count_pkey PRIMARY KEY (key, window_start, replica);


--
-- Name: entry_rate entry_rate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entry_rate
    ADD CONSTRAINT entry_rate_pkey PRIMARY KEY (key, window_start);


--
-- Name: execution_tag execution_tag_execution_id_tag_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_tag
    ADD CONSTRAINT execution_tag_execution_id_tag_key UNIQUE (execution_id, tag);


--
-- Name: execution_tag execution_tag_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_tag
    ADD CONSTRAINT execution_tag_pkey PRIMARY KEY (seq);


--
-- Name: image_build image_build_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.image_build
    ADD CONSTRAINT image_build_pkey PRIMARY KEY (image_ref);


--
-- Name: image_claim image_claim_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.image_claim
    ADD CONSTRAINT image_claim_pkey PRIMARY KEY (image_ref, claim_id);


--
-- Name: image_use image_use_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.image_use
    ADD CONSTRAINT image_use_pkey PRIMARY KEY (project_id, image_ref);


--
-- Name: infra_event infra_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_event
    ADD CONSTRAINT infra_event_pkey PRIMARY KEY (id);


--
-- Name: infra_health_look infra_health_look_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_health_look
    ADD CONSTRAINT infra_health_look_pkey PRIMARY KEY (project_id);


--
-- Name: infra_health_protocol infra_health_protocol_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_health_protocol
    ADD CONSTRAINT infra_health_protocol_pkey PRIMARY KEY (project_id, protocol);


--
-- Name: infra_lifecycle_command infra_lifecycle_command_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_lifecycle_command
    ADD CONSTRAINT infra_lifecycle_command_pkey PRIMARY KEY (id);


--
-- Name: infra_owner infra_owner_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.infra_owner
    ADD CONSTRAINT infra_owner_pkey PRIMARY KEY (project_id);


--
-- Name: install_domain_door install_domain_door_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.install_domain_door
    ADD CONSTRAINT install_domain_door_pkey PRIMARY KEY (singleton);


--
-- Name: install_domain install_domain_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.install_domain
    ADD CONSTRAINT install_domain_pkey PRIMARY KEY (name);


--
-- Name: install_pick install_pick_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.install_pick
    ADD CONSTRAINT install_pick_pkey PRIMARY KEY (project_id, step, field);


--
-- Name: instance_value instance_value_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.instance_value
    ADD CONSTRAINT instance_value_pkey PRIMARY KEY (project_id, instance_id, step, field);


--
-- Name: parked_fire parked_fire_fire_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parked_fire
    ADD CONSTRAINT parked_fire_fire_id_key UNIQUE (fire_id);


--
-- Name: parked_fire parked_fire_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parked_fire
    ADD CONSTRAINT parked_fire_pkey PRIMARY KEY (token, seq);


--
-- Name: project_code project_code_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_code
    ADD CONSTRAINT project_code_pkey PRIMARY KEY (project_id, binary_hash);


--
-- Name: project_definition project_definition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_definition
    ADD CONSTRAINT project_definition_pkey PRIMARY KEY (project_id, definition_hash);


--
-- Name: project_frontend project_frontend_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_frontend
    ADD CONSTRAINT project_frontend_pkey PRIMARY KEY (project_id, name);


--
-- Name: project project_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project
    ADD CONSTRAINT project_pkey PRIMARY KEY (id);


--
-- Name: project_version project_version_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_version
    ADD CONSTRAINT project_version_pkey PRIMARY KEY (project_id, id);


--
-- Name: public_file_link public_file_link_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.public_file_link
    ADD CONSTRAINT public_file_link_pkey PRIMARY KEY (token);


--
-- Name: role_loop_due role_loop_due_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.role_loop_due
    ADD CONSTRAINT role_loop_due_pkey PRIMARY KEY (role, loop_name);


--
-- Name: run_log run_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_log
    ADD CONSTRAINT run_log_pkey PRIMARY KEY (execution_id, seq);


--
-- Name: run run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run
    ADD CONSTRAINT run_pkey PRIMARY KEY (execution_id);


--
-- Name: run_search run_search_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_search
    ADD CONSTRAINT run_search_pkey PRIMARY KEY (execution_id);


--
-- Name: run_search_queue run_search_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_search_queue
    ADD CONSTRAINT run_search_queue_pkey PRIMARY KEY (execution_id);


--
-- Name: run_selection run_selection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_selection
    ADD CONSTRAINT run_selection_pkey PRIMARY KEY (digest);


--
-- Name: runtime_file_part runtime_file_part_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_file_part
    ADD CONSTRAINT runtime_file_part_pkey PRIMARY KEY (key, part_number);


--
-- Name: runtime_file runtime_file_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_file
    ADD CONSTRAINT runtime_file_pkey PRIMARY KEY (key);


--
-- Name: service_events_recipe service_events_recipe_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_events_recipe
    ADD CONSTRAINT service_events_recipe_pkey PRIMARY KEY (service, recipe_hash);


--
-- Name: signal signal_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signal
    ADD CONSTRAINT signal_pkey PRIMARY KEY (token);


--
-- Name: signal_subscription signal_subscription_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signal_subscription
    ADD CONSTRAINT signal_subscription_pkey PRIMARY KEY (id);


--
-- Name: signal_token signal_token_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signal_token
    ADD CONSTRAINT signal_token_pkey PRIMARY KEY (id);


--
-- Name: signal_token signal_token_token_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signal_token
    ADD CONSTRAINT signal_token_token_hash_key UNIQUE (token_hash);


--
-- Name: storage_sweep storage_sweep_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_sweep
    ADD CONSTRAINT storage_sweep_pkey PRIMARY KEY (execution_id);


--
-- Name: task task_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT task_pkey PRIMARY KEY (id);


--
-- Name: trigger_setup trigger_setup_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trigger_setup
    ADD CONSTRAINT trigger_setup_pkey PRIMARY KEY (execution_id);


--
-- Name: unanswered_alert unanswered_alert_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unanswered_alert
    ADD CONSTRAINT unanswered_alert_pkey PRIMARY KEY (one);


--
-- Name: unanswered_call unanswered_call_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unanswered_call
    ADD CONSTRAINT unanswered_call_pkey PRIMARY KEY (callee);


--
-- Name: version_build version_build_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.version_build
    ADD CONSTRAINT version_build_pkey PRIMARY KEY (id);


--
-- Name: version_runs version_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.version_runs
    ADD CONSTRAINT version_runs_pkey PRIMARY KEY (project_id, source_version, lane);


--
-- Name: weft_migration weft_migration_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weft_migration
    ADD CONSTRAINT weft_migration_pkey PRIMARY KEY (group_name, id);


--
-- Name: weft_news weft_news_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weft_news
    ADD CONSTRAINT weft_news_pkey PRIMARY KEY (seq);


--
-- Name: weft_schema_stamp weft_schema_stamp_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weft_schema_stamp
    ADD CONSTRAINT weft_schema_stamp_pkey PRIMARY KEY (group_name);


--
-- Name: worker_lease worker_lease_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.worker_lease
    ADD CONSTRAINT worker_lease_pkey PRIMARY KEY (replica);


--
-- Name: access_connect_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_connect_created ON public.access_connect USING btree (created_at);


--
-- Name: access_connect_result_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_connect_result_created ON public.access_connect_result USING btree (created_at);


--
-- Name: access_grant_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_grant_instance ON public.access_grant USING btree (project_id, instance_id) WHERE (instance_id IS NOT NULL);


--
-- Name: access_grant_published; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX access_grant_published ON public.access_grant USING btree (tenant_id, project_id, published_by_node, service, instance_id) NULLS NOT DISTINCT WHERE (published_by_node IS NOT NULL);


--
-- Name: access_grant_service_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_grant_service_account ON public.access_grant USING btree (service, provider_account);


--
-- Name: access_grant_tenant_service; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_grant_tenant_service ON public.access_grant USING btree (tenant_id, service);


--
-- Name: access_picker_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX access_picker_created ON public.access_picker USING btree (created_at);


--
-- Name: idx_alarm_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_alarm_due ON public.alarm USING btree (at_unix_ms);


--
-- Name: idx_asset_reference_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_asset_reference_key ON public.asset_reference USING btree (key);


--
-- Name: idx_door_count_window; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_door_count_window ON public.door_count USING btree (window_start);


--
-- Name: idx_execution_tag_tag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_execution_tag_tag ON public.execution_tag USING btree (tag);


--
-- Name: idx_infra_event_chrono; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_event_chrono ON public.infra_event USING btree (id);


--
-- Name: idx_infra_event_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_event_project ON public.infra_event USING btree (project_id);


--
-- Name: idx_infra_health_look_retry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_health_look_retry ON public.infra_health_look USING btree (retry_at_unix) WHERE (retry_at_unix IS NOT NULL);


--
-- Name: idx_infra_health_protocol_retry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_health_protocol_retry ON public.infra_health_protocol USING btree (next_retry_at_unix) WHERE ((gave_up_at_unix IS NULL) AND (next_retry_at_unix IS NOT NULL));


--
-- Name: idx_infra_node_copy; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_infra_node_copy ON public.infra_node USING btree (project_id, node_id, instance_id) NULLS NOT DISTINCT;


--
-- Name: idx_infra_node_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_node_project ON public.infra_node USING btree (project_id);


--
-- Name: idx_infra_owner_replica; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_infra_owner_replica ON public.infra_owner USING btree (supervisor_replica);


--
-- Name: idx_lifecycle_cmd_dispatcher_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lifecycle_cmd_dispatcher_claim ON public.infra_lifecycle_command USING btree (id) WHERE ((completed_at_unix IS NULL) AND (claimed_by_replica IS NULL) AND ((drain_by_unix IS NOT NULL) OR (verb = ANY (ARRAY['deactivate'::text, 'reactivate'::text, 'upgrade'::text]))));


--
-- Name: idx_lifecycle_cmd_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lifecycle_cmd_pending ON public.infra_lifecycle_command USING btree (tenant_id) WHERE (completed_at_unix IS NULL);


--
-- Name: idx_lifecycle_cmd_supervisor_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lifecycle_cmd_supervisor_claim ON public.infra_lifecycle_command USING btree (id) WHERE ((completed_at_unix IS NULL) AND (verb = ANY (ARRAY['apply'::text, 'stop'::text, 'terminate'::text])));


--
-- Name: idx_project_api_port; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_project_api_port ON public.project USING btree (api_port) WHERE (api_port IS NOT NULL);


--
-- Name: idx_project_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_project_tenant ON public.project USING btree (tenant_id);


--
-- Name: idx_project_version_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_project_version_parent ON public.project_version USING btree (project_id, parent_id);


--
-- Name: idx_runtime_file_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_file_expiry ON public.runtime_file USING btree (expires_at_unix) WHERE (expires_at_unix IS NOT NULL);


--
-- Name: idx_runtime_file_identity; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_file_identity ON public.runtime_file USING btree (regexp_replace(key, '/[^/]+$'::text, ''::text), identity) WHERE (identity IS NOT NULL);


--
-- Name: idx_runtime_file_last_replacement; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_file_last_replacement ON public.runtime_file USING btree (last_replacement) WHERE (last_replacement IS NOT NULL);


--
-- Name: idx_runtime_file_one_replacement; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_file_one_replacement ON public.runtime_file USING btree (replaces) WHERE (replaces IS NOT NULL);


--
-- Name: idx_runtime_file_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_file_tenant ON public.runtime_file USING btree (tenant_id);


--
-- Name: idx_signal_access_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_access_id ON public.signal USING btree (access_id) WHERE (access_id IS NOT NULL);


--
-- Name: idx_signal_access_stale; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_access_stale ON public.signal USING btree (token) WHERE access_stale;


--
-- Name: idx_signal_activation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_activation ON public.signal USING btree (project_id, activation_trigger, instance_id) WHERE (activation_trigger IS NOT NULL);


--
-- Name: idx_signal_consumer_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_consumer_kind ON public.signal USING btree (consumer_kind);


--
-- Name: idx_signal_entry_node; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_signal_entry_node ON public.signal USING btree (project_id, node_id, instance_id) NULLS NOT DISTINCT WHERE (is_resume = false);


--
-- Name: idx_signal_execution_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_execution_id ON public.signal USING btree (execution_id);


--
-- Name: idx_signal_held; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_held ON public.signal USING btree (held_by) WHERE holds;


--
-- Name: idx_signal_mount_path; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_signal_mount_path ON public.signal USING btree (mount_path, mount_methods) WHERE (mount_path IS NOT NULL);


--
-- Name: idx_signal_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_project ON public.signal USING btree (project_id);


--
-- Name: idx_signal_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_tenant ON public.signal USING btree (tenant_id);


--
-- Name: idx_signal_token_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_signal_token_tenant ON public.signal_token USING btree (tenant_id);


--
-- Name: idx_task_claimed_expired; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_claimed_expired ON public.task USING btree (claimed_until_unix) WHERE (status = 'claimed'::text);


--
-- Name: idx_task_dedup_live; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_task_dedup_live ON public.task USING btree (tenant_id, kind, dedup_key) WHERE ((dedup_key IS NOT NULL) AND (status = ANY (ARRAY['pending'::text, 'claimed'::text])));


--
-- Name: idx_task_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_pending ON public.task USING btree (created_at_unix) WHERE (status = 'pending'::text);


--
-- Name: idx_task_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_project ON public.task USING btree (project_id) WHERE (project_id IS NOT NULL);


--
-- Name: idx_task_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_tenant ON public.task USING btree (tenant_id);


--
-- Name: idx_task_terminal_completed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_terminal_completed ON public.task USING btree (completed_at_unix) WHERE (status = ANY (ARRAY['complete'::text, 'failed'::text]));


--
-- Name: idx_trigger_bake_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_trigger_bake_key ON public.trigger_bake USING btree (project_id, instance_id, program_hash) NULLS NOT DISTINCT;


--
-- Name: idx_trigger_setup_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trigger_setup_project ON public.trigger_setup USING btree (project_id);


--
-- Name: idx_worker_lease_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_worker_lease_project ON public.worker_lease USING btree (project_id, leased_until_unix);


--
-- Name: install_pick_grant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX install_pick_grant ON public.install_pick USING btree (grant_id);


--
-- Name: instance_value_grant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX instance_value_grant ON public.instance_value USING btree (grant_id) WHERE (grant_id IS NOT NULL);


--
-- Name: parked_fire_handed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX parked_fire_handed ON public.parked_fire USING btree (execution_id) WHERE (execution_id IS NOT NULL);


--
-- Name: run_end_unhandled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_end_unhandled ON public.run USING btree (project_id) WHERE ((state = 'ended'::text) AND (watch_end OR holds_signals));


--
-- Name: run_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_expiry ON public.run USING btree (keep_until) WHERE (state = 'ended'::text);


--
-- Name: run_in_flight; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_in_flight ON public.run USING btree (owner) WHERE (state = 'running'::text);


--
-- Name: run_listing; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_listing ON public.run USING btree (project_id, started_at DESC);


--
-- Name: run_live; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_live ON public.run USING btree (project_id) WHERE (state <> 'ended'::text);


--
-- Name: run_queued; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_queued ON public.run USING btree (started_at) WHERE (state = 'queued'::text);


--
-- Name: run_search_nodes; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_search_nodes ON public.run_search USING gin (nodes);


--
-- Name: run_search_words; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX run_search_words ON public.run_search USING gin (words);


--
-- Name: signal_subscription_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX signal_subscription_expiry ON public.signal_subscription USING btree (expires_at);


--
-- Name: signal_subscription_signal; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX signal_subscription_signal ON public.signal_subscription USING btree (signal_token);


--
-- Name: trigger_activation_execution_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX trigger_activation_execution_id ON public.trigger_activation USING btree (activating_execution_id) WHERE (activating_execution_id IS NOT NULL);


--
-- Name: trigger_activation_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX trigger_activation_key ON public.trigger_activation USING btree (project_id, trigger, instance_id) NULLS NOT DISTINCT;


--
-- Name: trigger_activation_transitional; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX trigger_activation_transitional ON public.trigger_activation USING btree (status) WHERE (status = ANY (ARRAY['activating'::text, 'deactivating'::text]));


--
-- Name: uq_lifecycle_cmd_pending_apply; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_lifecycle_cmd_pending_apply ON public.infra_lifecycle_command USING btree (project_id, node_id, instance_id) NULLS NOT DISTINCT WHERE ((completed_at_unix IS NULL) AND (verb = 'apply'::text));


--
-- Name: version_build_one_waiting; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX version_build_one_waiting ON public.version_build USING btree (project_id) WHERE (state = 'waiting'::text);


--
-- Name: weft_news_refused_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX weft_news_refused_idx ON public.weft_news USING btree (refused_at, seq) WHERE (refused_at IS NOT NULL);


--
-- Name: access_grant access_grant_gone_stales_signals; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER access_grant_gone_stales_signals AFTER DELETE ON public.access_grant FOR EACH ROW EXECUTE FUNCTION public.signal_access_stale();


--
-- Name: access_grant access_grant_notify; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER access_grant_notify AFTER INSERT OR DELETE OR UPDATE ON public.access_grant FOR EACH ROW EXECUTE FUNCTION public.access_notify();


--
-- Name: access_grant access_grant_stales_signals; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER access_grant_stales_signals AFTER UPDATE OF reconnects ON public.access_grant FOR EACH ROW WHEN ((new.reconnects IS DISTINCT FROM old.reconnects)) EXECUTE FUNCTION public.signal_access_stale();


--
-- Name: infra_lifecycle_command infra_command_notify_on_cancel; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_command_notify_on_cancel AFTER UPDATE OF cancel_requested ON public.infra_lifecycle_command FOR EACH ROW WHEN ((new.cancel_requested AND (NOT old.cancel_requested))) EXECUTE FUNCTION public.infra_command_cancel_notify();


--
-- Name: infra_lifecycle_command infra_command_notify_on_done; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_command_notify_on_done AFTER UPDATE OF completed_at_unix ON public.infra_lifecycle_command FOR EACH ROW WHEN (((new.completed_at_unix IS NOT NULL) AND (old.completed_at_unix IS NULL))) EXECUTE FUNCTION public.infra_command_notify();


--
-- Name: infra_lifecycle_command infra_command_notify_on_drained; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_command_notify_on_drained AFTER UPDATE OF drain_by_unix ON public.infra_lifecycle_command FOR EACH ROW WHEN (((new.drain_by_unix IS NULL) AND (old.drain_by_unix IS NOT NULL))) EXECUTE FUNCTION public.infra_command_notify();


--
-- Name: infra_lifecycle_command infra_command_notify_on_issue; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_command_notify_on_issue AFTER INSERT ON public.infra_lifecycle_command FOR EACH ROW EXECUTE FUNCTION public.infra_command_notify();


--
-- Name: infra_event infra_event_notify_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_event_notify_on_insert AFTER INSERT ON public.infra_event FOR EACH ROW EXECUTE FUNCTION public.infra_event_notify();


--
-- Name: infra_node infra_node_status_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_node_status_on_change AFTER UPDATE OF status, endpoints_json, public_paths_json, baked_json ON public.infra_node FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.endpoints_json IS DISTINCT FROM old.endpoints_json) OR (new.public_paths_json IS DISTINCT FROM old.public_paths_json) OR (new.baked_json IS DISTINCT FROM old.baked_json))) EXECUTE FUNCTION public.infra_node_status_notify();


--
-- Name: infra_node infra_node_status_on_row; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER infra_node_status_on_row AFTER INSERT OR DELETE ON public.infra_node FOR EACH ROW EXECUTE FUNCTION public.infra_node_status_notify();


--
-- Name: install_domain install_domain_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER install_domain_changed AFTER INSERT OR DELETE ON public.install_domain FOR EACH ROW EXECUTE FUNCTION public.install_domain_notify();


--
-- Name: install_pick install_pick_notify; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER install_pick_notify AFTER INSERT OR DELETE OR UPDATE ON public.install_pick FOR EACH ROW EXECUTE FUNCTION public.access_notify();


--
-- Name: instance_value instance_value_notify; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER instance_value_notify AFTER INSERT OR DELETE OR UPDATE ON public.instance_value FOR EACH ROW EXECUTE FUNCTION public.access_notify();


--
-- Name: signal parked_fire_drop_on_signal_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER parked_fire_drop_on_signal_delete AFTER DELETE ON public.signal FOR EACH ROW EXECUTE FUNCTION public.parked_fire_drop_with_signal();


--
-- Name: parked_fire parked_fire_notify_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER parked_fire_notify_on_insert AFTER INSERT ON public.parked_fire FOR EACH ROW EXECUTE FUNCTION public.parked_fire_notify();


--
-- Name: project project_declared_infra_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER project_declared_infra_on_change AFTER UPDATE OF project_json ON public.project FOR EACH ROW WHEN ((new.project_json IS DISTINCT FROM old.project_json)) EXECUTE FUNCTION public.project_declared_infra_notify();


--
-- Name: project project_routes_on_row; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER project_routes_on_row AFTER INSERT OR DELETE ON public.project FOR EACH ROW EXECUTE FUNCTION public.routes_notify_tenant();


--
-- Name: project project_worker_settings_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER project_worker_settings_on_change AFTER UPDATE OF worker_settings_json ON public.project FOR EACH ROW WHEN ((new.worker_settings_json IS DISTINCT FROM old.worker_settings_json)) EXECUTE FUNCTION public.project_worker_settings_notify();


--
-- Name: project project_worker_settings_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER project_worker_settings_on_delete AFTER DELETE ON public.project FOR EACH ROW EXECUTE FUNCTION public.project_worker_settings_notify();


--
-- Name: signal signal_held_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_held_on_change AFTER UPDATE OF holds, held_failed ON public.signal FOR EACH ROW WHEN (((new.holds IS DISTINCT FROM old.holds) OR (new.held_failed IS DISTINCT FROM old.held_failed))) EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: signal signal_held_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_held_on_delete AFTER DELETE ON public.signal FOR EACH ROW WHEN (old.holds) EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: signal signal_held_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_held_on_insert AFTER INSERT ON public.signal FOR EACH ROW WHEN (new.holds) EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: signal signal_held_on_release; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_held_on_release AFTER UPDATE OF held_by ON public.signal FOR EACH ROW WHEN ((new.holds AND (new.held_by IS NULL) AND (old.held_by IS NOT NULL))) EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: signal signal_routes_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_routes_on_change AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json, auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id, activation_trigger ON public.signal FOR EACH ROW WHEN (((new.surface_kind = 'public_entry'::text) OR (old.surface_kind = 'public_entry'::text))) EXECUTE FUNCTION public.routes_notify_tenant();


--
-- Name: signal signal_routes_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_routes_on_delete AFTER DELETE ON public.signal FOR EACH ROW WHEN ((old.surface_kind = 'public_entry'::text)) EXECUTE FUNCTION public.routes_notify_tenant();


--
-- Name: signal signal_routes_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_routes_on_insert AFTER INSERT ON public.signal FOR EACH ROW WHEN ((new.surface_kind = 'public_entry'::text)) EXECUTE FUNCTION public.routes_notify_tenant();


--
-- Name: signal signal_triggers_on_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_triggers_on_change AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json, auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id, activation_trigger, held_by ON public.signal FOR EACH ROW WHEN ((NOT new.is_resume)) EXECUTE FUNCTION public.triggers_notify_project();


--
-- Name: signal signal_triggers_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_triggers_on_delete AFTER DELETE ON public.signal FOR EACH ROW WHEN ((NOT old.is_resume)) EXECUTE FUNCTION public.triggers_notify_project();


--
-- Name: signal signal_triggers_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER signal_triggers_on_insert AFTER INSERT ON public.signal FOR EACH ROW WHEN ((NOT new.is_resume)) EXECUTE FUNCTION public.triggers_notify_project();


--
-- Name: storage_sweep storage_sweep_notify_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER storage_sweep_notify_on_insert AFTER INSERT ON public.storage_sweep FOR EACH ROW EXECUTE FUNCTION public.storage_sweep_notify();


--
-- Name: task task_ready_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER task_ready_on_insert AFTER INSERT ON public.task FOR EACH ROW WHEN ((new.status = 'pending'::text)) EXECUTE FUNCTION public.task_ready_notify();


--
-- Name: task task_ready_on_pending; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER task_ready_on_pending AFTER UPDATE OF status ON public.task FOR EACH ROW WHEN (((new.status = 'pending'::text) AND (old.status IS DISTINCT FROM 'pending'::text))) EXECUTE FUNCTION public.task_ready_notify();


--
-- Name: trigger_activation trigger_activation_held_on_row; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_held_on_row AFTER INSERT OR DELETE ON public.trigger_activation FOR EACH ROW EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: trigger_activation trigger_activation_held_on_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_held_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON public.trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION public.signal_held_notify();


--
-- Name: trigger_activation trigger_activation_routes_on_row; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_routes_on_row AFTER INSERT OR DELETE ON public.trigger_activation FOR EACH ROW EXECUTE FUNCTION public.trigger_activation_routes_notify();


--
-- Name: trigger_activation trigger_activation_routes_on_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_routes_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON public.trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION public.trigger_activation_routes_notify();


--
-- Name: trigger_activation trigger_activation_triggers_on_row; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_triggers_on_row AFTER INSERT OR DELETE ON public.trigger_activation FOR EACH ROW EXECUTE FUNCTION public.trigger_activation_triggers_notify();


--
-- Name: trigger_activation trigger_activation_triggers_on_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_activation_triggers_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON public.trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION public.trigger_activation_triggers_notify();


--
-- Name: worker_lease worker_lease_alive_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER worker_lease_alive_on_insert AFTER INSERT ON public.worker_lease FOR EACH ROW EXECUTE FUNCTION public.worker_lease_alive_notify();


--
-- Name: worker_lease worker_lease_alive_on_renewal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER worker_lease_alive_on_renewal AFTER UPDATE OF leased_until_unix ON public.worker_lease FOR EACH ROW WHEN ((old.leased_until_unix < (EXTRACT(epoch FROM now()))::bigint)) EXECUTE FUNCTION public.worker_lease_alive_notify();


--
-- Name: worker_lease worker_lease_workers_back_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER worker_lease_workers_back_on_insert AFTER INSERT ON public.worker_lease FOR EACH ROW EXECUTE FUNCTION public.worker_lease_workers_back_notify();


--
-- Name: worker_lease worker_lease_workers_back_on_renewal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER worker_lease_workers_back_on_renewal AFTER UPDATE OF leased_until_unix ON public.worker_lease FOR EACH ROW WHEN ((old.leased_until_unix < (EXTRACT(epoch FROM now()))::bigint)) EXECUTE FUNCTION public.worker_lease_workers_back_notify();


--
-- Name: install_domain install_domain_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.install_domain
    ADD CONSTRAINT install_domain_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project(id) ON DELETE CASCADE;


--
-- Name: install_pick install_pick_grant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.install_pick
    ADD CONSTRAINT install_pick_grant_id_fkey FOREIGN KEY (grant_id) REFERENCES public.access_grant(id) ON DELETE CASCADE;


--
-- Name: instance_value instance_value_grant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.instance_value
    ADD CONSTRAINT instance_value_grant_id_fkey FOREIGN KEY (grant_id) REFERENCES public.access_grant(id) ON DELETE CASCADE;


--
-- Name: project_code project_code_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_code
    ADD CONSTRAINT project_code_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project(id) ON DELETE CASCADE;


--
-- Name: project_frontend project_frontend_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_frontend
    ADD CONSTRAINT project_frontend_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project(id) ON DELETE CASCADE;


--
-- Name: public_file_link public_file_link_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.public_file_link
    ADD CONSTRAINT public_file_link_key_fkey FOREIGN KEY (key) REFERENCES public.runtime_file(key) ON DELETE CASCADE;


--
-- Name: runtime_file_part runtime_file_part_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_file_part
    ADD CONSTRAINT runtime_file_part_key_fkey FOREIGN KEY (key) REFERENCES public.runtime_file(key) ON DELETE CASCADE;


--
-- Name: trigger_activation trigger_activation_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trigger_activation
    ADD CONSTRAINT trigger_activation_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: weft_migration; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.weft_migration VALUES ('_history', 'serverless', '', '2026-10-17 19:34:39.408428+00');


--
-- Data for Name: weft_schema_stamp; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.weft_schema_stamp VALUES ('exec_event', 'serverless-final');


--
-- PostgreSQL database dump complete
--


