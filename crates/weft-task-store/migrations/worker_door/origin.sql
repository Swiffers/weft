CREATE TABLE IF NOT EXISTS worker_lease (
            replica TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            -- The image it runs (the program's binary hash); NULL for a
            -- process that runs no program (a node test's).
            binary_hash TEXT,
            -- How many runs it drives right now, per trigger token
            -- (`{"<token>": n}`): what a drain counts as work in flight.
            in_flight JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- When its lease last came alive (made, or renewed after it
            -- had lapsed), on the database's clock: a queued run its
            -- project's workers could not be reached for is tried again
            -- once a worker came alive after its tries stopped
            -- (`weft_dispatcher::delivery`).
            alive_since BIGINT NOT NULL DEFAULT 0
        );
CREATE INDEX IF NOT EXISTS idx_worker_lease_project ON worker_lease(project_id, leased_until_unix);
CREATE OR REPLACE FUNCTION worker_lease_alive_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_lease_alive', NEW.replica);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS worker_lease_alive_on_insert ON worker_lease;
CREATE TRIGGER worker_lease_alive_on_insert
            AFTER INSERT ON worker_lease
            FOR EACH ROW
            EXECUTE FUNCTION worker_lease_alive_notify();
DROP TRIGGER IF EXISTS worker_lease_alive_on_renewal ON worker_lease;
CREATE TRIGGER worker_lease_alive_on_renewal
            AFTER UPDATE OF leased_until_unix ON worker_lease
            FOR EACH ROW
            WHEN (OLD.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT)
            EXECUTE FUNCTION worker_lease_alive_notify();
CREATE UNLOGGED TABLE IF NOT EXISTS door_count (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            replica TEXT NOT NULL,
            project_id UUID NOT NULL,
            hits BIGINT NOT NULL,
            PRIMARY KEY (key, window_start, replica)
        );
CREATE INDEX IF NOT EXISTS idx_door_count_window ON door_count(window_start);
CREATE OR REPLACE FUNCTION weft_door_tick(
                p_replica TEXT, p_project UUID, p_tenant TEXT, p_binary_hash TEXT, p_leased_until BIGINT,
                p_in_flight JSONB, p_window BIGINT, p_keys TEXT[], p_hits BIGINT[], p_shortens BOOLEAN
            ) RETURNS JSONB AS $$
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
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION weft_door_tick(
                p_replica TEXT, p_project UUID, p_tenant TEXT, p_binary_hash TEXT, p_leased_until BIGINT,
                p_in_flight JSONB, p_window BIGINT, p_keys TEXT[], p_hits BIGINT[], p_tokens TEXT[]
            ) RETURNS JSONB AS $$
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
            $$ LANGUAGE sql;
