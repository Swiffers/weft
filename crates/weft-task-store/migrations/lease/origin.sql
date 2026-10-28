CREATE TABLE IF NOT EXISTS lease (
            -- What is held (`<what>/<scope>`: `sweep/<loop>`,
            -- `front/<project>`, `signal_mount/<tenant>`).
            name TEXT PRIMARY KEY,
            -- The one holding it: a token of its own per hold, so two
            -- holds in one process are told apart too.
            holder UUID NOT NULL,
            -- Until when, in unix milliseconds on the database's clock;
            -- renewed while the holder works. Past it, anyone takes it.
            until_ms BIGINT NOT NULL
        );
