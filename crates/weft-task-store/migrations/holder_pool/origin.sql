CREATE TABLE IF NOT EXISTS holder_pool (
            -- At most one row, written by the first sizing that went
            -- through: before it nobody knows the pool's count, so that
            -- sizing asks the platform.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            -- The count the platform's pool was last set to.
            copies BIGINT NOT NULL
        );
