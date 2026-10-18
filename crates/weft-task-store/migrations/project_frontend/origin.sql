CREATE TABLE IF NOT EXISTS project_frontend (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            -- Where it runs: 'cloud_run' (the install made its service) or
            -- 'external'.
            host TEXT NOT NULL CHECK (host IN ('cloud_run', 'external')),
            -- The GitHub repository that deploys a frontend the install
            -- hosts (its name, and the id GitHub gave it, which access is
            -- granted to), its service there, and where visitors reach it.
            repo TEXT,
            repo_id BIGINT,
            service TEXT,
            url TEXT,
            -- The caller token it calls with (`signal_token.id`), and the
            -- ones made to replace it and not put in place yet. NULL for a
            -- hosted frontend until its deploy puts its first in place.
            token_id UUID,
            pending_token_ids UUID[] NOT NULL DEFAULT '{}',
            added_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, name),
            CHECK ((host = 'cloud_run') = (repo IS NOT NULL)),
            CHECK ((repo IS NULL) = (repo_id IS NULL))
        );
