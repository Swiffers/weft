CREATE TABLE IF NOT EXISTS infra_owner (
            project_id          UUID PRIMARY KEY,
            supervisor_replica TEXT NOT NULL,
            tenant_id           TEXT NOT NULL,
            leased_until_unix   BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_infra_owner_replica
             ON infra_owner(supervisor_replica);
