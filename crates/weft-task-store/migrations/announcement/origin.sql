CREATE UNLOGGED TABLE IF NOT EXISTS weft_announcement (
            channel TEXT NOT NULL,
            payload TEXT NOT NULL
        );
CREATE TABLE IF NOT EXISTS weft_news (
            seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            project_id UUID NOT NULL,
            run_id UUID NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('answer', 'cancel')),
            refused_at TIMESTAMPTZ
        );
CREATE INDEX IF NOT EXISTS weft_news_refused_idx ON weft_news (refused_at, seq) WHERE refused_at IS NOT NULL;
CREATE OR REPLACE FUNCTION weft_news_refuse(p_seqs BIGINT[]) RETURNS BIGINT AS $$
            DECLARE
                v_marked BIGINT;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                UPDATE weft_news SET refused_at = clock_timestamp() WHERE seq = ANY(p_seqs);
                GET DIAGNOSTICS v_marked = ROW_COUNT;
                RETURN v_marked;
            END;
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION weft_news_refused(p_limit BIGINT, p_skip BIGINT[], p_callee TEXT)
            RETURNS TABLE (seq BIGINT, project_id UUID, run_id UUID, kind TEXT) AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                IF NOT EXISTS (SELECT 1 FROM weft_news n WHERE n.refused_at IS NOT NULL) THEN
                    DELETE FROM unanswered_call u WHERE u.callee = p_callee;
                    RETURN;
                END IF;
                RETURN QUERY SELECT n.seq, n.project_id, n.run_id, n.kind FROM weft_news n
                    WHERE n.refused_at IS NOT NULL AND n.seq <> ALL(p_skip)
                    ORDER BY n.refused_at, n.seq LIMIT p_limit;
            END;
            $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION weft_tell(p_project UUID, p_run UUID, p_kind TEXT) RETURNS VOID AS $$
            INSERT INTO weft_news (project_id, run_id, kind) VALUES (p_project, p_run, p_kind)
            $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION weft_announce(p_channel TEXT, p_payload TEXT) RETURNS VOID AS $$
            INSERT INTO weft_announcement (channel, payload) VALUES (p_channel, p_payload)
            $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION weft_flush_announcements(p_limit INTEGER) RETURNS INTEGER AS $$
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
            $$ LANGUAGE plpgsql;
