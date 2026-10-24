CREATE TABLE IF NOT EXISTS maintenance (
            -- One row while the install is in maintenance: `weft
            -- maintenance on` writes it, the lift deletes it once nothing
            -- is left to bring back. Every verb that turns something on
            -- reads it.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            since_unix BIGINT NOT NULL
        );
CREATE TABLE IF NOT EXISTS maintenance_copy (
            -- An infra copy `weft maintenance on` stopped, which the lift
            -- starts again: its project, its node's place as
            -- `infra_node.node_id` spells it, and the instance whose copy
            -- it is (NULL for the shared one).
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            node_id TEXT NOT NULL,
            instance_id TEXT,
            stopped_unix BIGINT NOT NULL
        );
CREATE UNIQUE INDEX IF NOT EXISTS maintenance_copy_key
             ON maintenance_copy (project_id, node_id, instance_id) NULLS NOT DISTINCT;
