ALTER TABLE task ADD COLUMN claim_rests_on uuid;

CREATE OR REPLACE FUNCTION weft_prune_finished_tasks(p_before bigint)
 RETURNS integer
 LANGUAGE sql
AS $function$
            WITH gone AS (
                DELETE FROM task WHERE id IN (
                    SELECT id FROM task
                    WHERE status IN ('complete', 'failed') AND completed_at_unix < p_before
                    LIMIT 500 FOR UPDATE SKIP LOCKED)
                RETURNING 1)
            SELECT count(*)::INTEGER FROM gone
            $function$
;
