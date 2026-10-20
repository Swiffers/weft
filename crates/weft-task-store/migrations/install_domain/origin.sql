CREATE TABLE IF NOT EXISTS install_domain (
            -- Lower case, as `weft_core::install::normalize_domain_name`
            -- leaves it.
            name TEXT PRIMARY KEY,
            -- What it serves: 'install', 'frontend' or 'api'.
            serves TEXT NOT NULL CHECK (serves IN ('install', 'frontend', 'api')),
            -- The project a frontend or API domain belongs to.
            project_id UUID REFERENCES project(id) ON DELETE CASCADE,
            -- Where a frontend runs (its service's https address).
            upstream TEXT,
            added_unix BIGINT NOT NULL,
            CHECK ((serves = 'install') = (project_id IS NULL)),
            CHECK ((serves = 'frontend') = (upstream IS NOT NULL))
        );
CREATE OR REPLACE FUNCTION install_domain_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_install_domains', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
CREATE TABLE IF NOT EXISTS install_domain_door (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            served TEXT[] NOT NULL,
            -- When the door last accepted `served` (unix ms, the
            -- database's clock).
            served_at_ms BIGINT NOT NULL,
            refused TEXT,
            refusals INT NOT NULL
        );
DROP TRIGGER IF EXISTS install_domain_changed ON install_domain;
CREATE TRIGGER install_domain_changed
            AFTER INSERT OR DELETE ON install_domain
            FOR EACH ROW
            EXECUTE FUNCTION install_domain_notify();
