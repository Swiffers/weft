CREATE TABLE IF NOT EXISTS parked_fire (
            -- The trigger's signal token, or the token a waiting run's
            -- answer resolves.
            token TEXT NOT NULL,
            seq BIGINT GENERATED ALWAYS AS IDENTITY,
            -- The event's identity: the run it starts is
            -- `weft_core::door_fire::run_of_fire(fire_id)`, so an event
            -- handed over twice is born once.
            fire_id UUID NOT NULL UNIQUE,
            -- What the trigger wakes with: every event waits here already
            -- processed by its kind (`/process`).
            payload JSONB NOT NULL,
            -- Who sent it, for a trigger somebody outside calls.
            caller TEXT,
            -- How many times it waited for room to run, and the moment
            -- it may be tried again (`park_backoff_secs`).
            attempts INTEGER NOT NULL DEFAULT 0,
            not_before BIGINT NOT NULL,
            -- Set while it waits on something no clock brings
            -- (`WaitsFor`, `<kind>: <reason>`): a value its instance has
            -- not given, infra the run reads, connections the install has
            -- not picked, or a hand-over that kept failing. Not retried on
            -- a timer: what it waits on hands it over.
            waits_for TEXT,
            -- Read by nothing: the instance refusal the previous release
            -- wrote where `waits_for` is now (its migration carries each
            -- into `waits_for`), which its replicas still write while an
            -- upgrade rolls out. The release after this one drops it.
            instance_gap JSONB,
            -- How many hand-overs in a row failed (`Turn::next`).
            failures INTEGER NOT NULL DEFAULT 0,
            -- An answer to a waiting run, rather than an event of an entry.
            is_resume BOOLEAN NOT NULL,
            -- An answer that is a person skipping the wait (its `/skip`
            -- door), not a value: `payload` is then null and unread.
            skipped BOOLEAN NOT NULL DEFAULT FALSE,
            -- Set on an answer already taken off its wait (the wait's
            -- signal is gone) for a run a worker drives: the run it is
            -- for. Its worker takes it; if the worker lets go of the run
            -- first, the letting go writes it into the run's record
            -- (`weft_journal::record::resolve_handed_in`).
            run_id UUID,
            PRIMARY KEY (token, seq)
        );
CREATE INDEX IF NOT EXISTS parked_fire_handed ON parked_fire (run_id) WHERE run_id IS NOT NULL;
CREATE OR REPLACE FUNCTION parked_fire_notify() RETURNS trigger AS $$
            DECLARE
                v_project UUID;
            BEGIN
                IF NEW.run_id IS NULL THEN
                    PERFORM weft_announce('weft_parked_fire', (SELECT s.project_id FROM signal s WHERE s.token = NEW.token)::text);
                ELSE
                    SELECT r.project_id INTO v_project FROM run r WHERE r.run_id = NEW.run_id;
                    PERFORM weft_announce('weft_parked_fire', v_project::text || ' ' || NEW.run_id::text);
                    PERFORM weft_tell(v_project, NEW.run_id, 'answer');
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS parked_fire_notify_on_insert ON parked_fire;
CREATE TRIGGER parked_fire_notify_on_insert
            AFTER INSERT ON parked_fire
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_notify();
CREATE OR REPLACE FUNCTION parked_fire_drop_with_signal() RETURNS trigger AS $$
            BEGIN
                DELETE FROM parked_fire WHERE token = OLD.token AND run_id IS NULL;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS parked_fire_drop_on_signal_delete ON signal;
CREATE TRIGGER parked_fire_drop_on_signal_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_drop_with_signal();
