CREATE TABLE IF NOT EXISTS alarm (
            -- The wake's name (`Wake::name`): the same key, time and
            -- generation is the same wake, set once however often it is
            -- asked for, and deleted by name when it is cancelled.
            name TEXT PRIMARY KEY,
            key TEXT NOT NULL,
            -- When it is due (unix milliseconds). A claimed wake has this
            -- pushed past its lease, so a delivery that never finishes
            -- comes due again.
            at_unix_ms BIGINT NOT NULL,
            -- The moment the wake was set for, what the receiver is told.
            set_for_unix_ms BIGINT NOT NULL,
            role TEXT NOT NULL,
            path TEXT NOT NULL,
            body JSONB NOT NULL,
            -- Deliveries claimed, whatever became of them: how long the
            -- next try waits.
            attempts INTEGER NOT NULL DEFAULT 0,
            -- Deliveries the receiver answered and failed
            -- (`weft_platform_traits::alarm::answered_by_receiver`): what
            -- the receiver is told, so one it never got spends none of
            -- its tries.
            answered INTEGER NOT NULL DEFAULT 0
        );
CREATE INDEX IF NOT EXISTS idx_alarm_due ON alarm(at_unix_ms);
