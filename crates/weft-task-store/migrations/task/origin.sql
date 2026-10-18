CREATE TABLE IF NOT EXISTS task (
            id UUID PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            project_id UUID,
            dedup_key TEXT,
            run_id UUID,
            tenant_id TEXT NOT NULL,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            -- Until when the claim holds without a heartbeat. NULL on a
            -- claim that rests on a run (`claim_rests_on`).
            claimed_until_unix BIGINT,
            -- The run a claimed task waits on while it waits (an
            -- activation's setup run, which may wait days on a person):
            -- its claimant stops renewing, and the claim holds for as
            -- long as that run goes (`weft_dispatcher::lapses`).
            claim_rests_on UUID,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT
        );
CREATE INDEX IF NOT EXISTS idx_task_pending
            ON task(created_at_unix)
            WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_task_claimed_expired
            ON task(claimed_until_unix)
            WHERE status = 'claimed';
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_dedup_live
            ON task(tenant_id, kind, dedup_key)
            WHERE dedup_key IS NOT NULL AND status IN ('pending', 'claimed');
CREATE INDEX IF NOT EXISTS idx_task_tenant ON task(tenant_id);
CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed');
CREATE OR REPLACE FUNCTION weft_lock_dedup(p_tenant TEXT, p_kind TEXT, p_dedup TEXT) RETURNS VOID AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended(p_tenant || '|' || p_kind || '|' || p_dedup, 0));
            END;
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION weft_prune_finished_tasks(p_before BIGINT) RETURNS INTEGER AS $$
            WITH gone AS (
                DELETE FROM task WHERE id IN (
                    SELECT id FROM task
                    WHERE status IN ('complete', 'failed') AND completed_at_unix < p_before
                    LIMIT 500 FOR UPDATE SKIP LOCKED)
                RETURNING 1)
            SELECT count(*)::INTEGER FROM gone
            $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION weft_enqueue_dedup(
                p_id UUID, p_kind TEXT, p_project UUID, p_dedup TEXT, p_run UUID,
                p_tenant TEXT, p_payload JSONB, p_now BIGINT,
                OUT task_id UUID, OUT inserted BOOLEAN
            ) AS $$
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
                INSERT INTO task (id, kind, status, project_id, dedup_key, run_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_run, p_tenant, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION task_ready_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_task_ready', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS task_ready_on_insert ON task;
CREATE TRIGGER task_ready_on_insert
            AFTER INSERT ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending')
            EXECUTE FUNCTION task_ready_notify();
DROP TRIGGER IF EXISTS task_ready_on_pending ON task;
CREATE TRIGGER task_ready_on_pending
            AFTER UPDATE OF status ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM 'pending')
            EXECUTE FUNCTION task_ready_notify();
