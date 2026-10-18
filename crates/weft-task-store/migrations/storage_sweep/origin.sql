-- Durable terminate-sweep queue: a row per ended run that stored
        -- files of its own, whose un-kept run files still need sweeping.
        -- Inserted by the write that ends the run (`weft_record_batch`,
        -- `weft_journal::record::append_locked_in`), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            run_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
CREATE OR REPLACE FUNCTION storage_sweep_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_storage_sweep', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS storage_sweep_notify_on_insert ON storage_sweep;
CREATE TRIGGER storage_sweep_notify_on_insert
            AFTER INSERT ON storage_sweep
            FOR EACH ROW
            EXECUTE FUNCTION storage_sweep_notify();
