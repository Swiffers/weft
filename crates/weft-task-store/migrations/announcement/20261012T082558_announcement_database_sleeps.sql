ALTER TABLE weft_news ADD COLUMN refused_at timestamp with time zone;

CREATE OR REPLACE FUNCTION weft_news_refuse(p_seqs bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_marked BIGINT;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                UPDATE weft_news SET refused_at = clock_timestamp() WHERE seq = ANY(p_seqs);
                GET DIAGNOSTICS v_marked = ROW_COUNT;
                RETURN v_marked;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_news_refused(p_limit bigint, p_skip bigint[], p_callee text)
 RETURNS TABLE(seq bigint, project_id uuid, execution_id uuid, kind text)
 LANGUAGE plpgsql
AS $function$
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
            $function$
;

CREATE INDEX weft_news_refused_idx ON weft_news USING btree (refused_at, seq) WHERE (refused_at IS NOT NULL);
