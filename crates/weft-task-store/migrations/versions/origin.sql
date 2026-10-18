CREATE TABLE IF NOT EXISTS project_version (
            id           TEXT NOT NULL,
            -- Deliberately NO foreign key to `project`: the tree's life is
            -- the store's to decide, not the database's. A project's
            -- removal (`ProjectStore::remove`) drops the tree explicitly,
            -- so the same id registered again never inherits a tree it did
            -- not make and no version keeps naming stored files of a
            -- project that no longer exists. The journal's runs go
            -- just before, so a removal leaves the person their files and
            -- nothing else.
            project_id   UUID NOT NULL,
            parent_id    TEXT,
            manifest     JSONB NOT NULL,
            label        TEXT,
            created_at   BIGINT NOT NULL,
            -- Recording order. `created_at` is whole seconds, so two
            -- versions recorded in one second tie on it; `versions()`
            -- lists by this instead.
            seq          BIGSERIAL,
            PRIMARY KEY (project_id, id)
        );
CREATE INDEX IF NOT EXISTS idx_project_version_parent ON project_version(project_id, parent_id);
CREATE TABLE IF NOT EXISTS version_runs (
            project_id     UUID NOT NULL,
            source_version TEXT NOT NULL,
            lane           TEXT NOT NULL,
            runs           BIGINT NOT NULL,
            last_run       UUID NOT NULL,
            PRIMARY KEY (project_id, source_version, lane)
        );
