CREATE TABLE IF NOT EXISTS database_throttle (
            -- Whose copies waited (`CoreRole::as_str`, or `machine` for a
            -- local install's one process).
            role TEXT PRIMARY KEY,
            -- The first wait written of this stretch and the last, in
            -- unix milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL,
            -- The waits counted in this stretch, every copy together.
            waits BIGINT NOT NULL,
            -- What the database said it takes, and one copy's share of
            -- it, as the last writer read them at its start
            -- (`crate::budget`).
            max_connections INTEGER NOT NULL,
            share INTEGER NOT NULL
        );
