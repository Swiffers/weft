CREATE TABLE IF NOT EXISTS unanswered_call (
            -- What does not answer (`Callee::key`).
            callee TEXT PRIMARY KEY,
            -- What the failing work does, in a person's words, when the
            -- key alone does not say (a role's loop).
            label TEXT,
            -- The error, as the caller logged it.
            error TEXT NOT NULL,
            -- The first failure of this stretch and the last written, in
            -- unix milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL,
            -- When this stretch was logged as an alert (unix ms on the
            -- database's clock); NULL until it is.
            alerted_ms BIGINT
        );
CREATE TABLE IF NOT EXISTS unanswered_alert (
            -- At most one row: the install alerts on its stuck failures
            -- (`alert_after`); no row, it does not.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            -- How long a failure weft still retries is current before it
            -- counts as stuck, in milliseconds.
            after_ms BIGINT NOT NULL
        );
