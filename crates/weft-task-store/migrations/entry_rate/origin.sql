CREATE UNLOGGED TABLE IF NOT EXISTS entry_rate (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            hits INTEGER NOT NULL,
            PRIMARY KEY (key, window_start)
        );
CREATE OR REPLACE FUNCTION weft_rate_hit(p_key TEXT, p_window_start BIGINT) RETURNS INTEGER AS $$
            DELETE FROM entry_rate WHERE window_start < p_window_start - 60;
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $$ LANGUAGE sql;
