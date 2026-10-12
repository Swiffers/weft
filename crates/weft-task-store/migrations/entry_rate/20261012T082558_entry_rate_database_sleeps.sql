CREATE OR REPLACE FUNCTION weft_rate_hit(p_key text, p_window_start bigint)
 RETURNS integer
 LANGUAGE sql
AS $function$
            DELETE FROM entry_rate WHERE window_start < p_window_start - 60;
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $function$
;
