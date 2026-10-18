CREATE TABLE IF NOT EXISTS access_grant (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- The resolved app credentials (client id + secret + extras),
            -- snapshotted at connect so runtime refresh needs no lookup.
            -- SEALED (crypt.rs). NULL for services that use no OAuth app
            -- (a pasted token).
            registration_sealed TEXT,
            -- The app snapshot's client id, plain: it is public by
            -- OAuth's design, and event routing filters on it in SQL.
            client_id TEXT,
            -- NULL for an exclusive-class shared grant.
            project_id UUID,
            -- The AccessSpec snapshot: refresh/auth need no catalog.
            spec_json JSONB NOT NULL,
            -- The stored values (token, refresh_token, captures, pasted
            -- fields), SEALED (crypt.rs). What a resolution hands over
            -- is everything here EXCEPT the store's own keep-alive
            -- material (the refresh token), so a credential of any
            -- shape travels whole.
            values_sealed TEXT NOT NULL,
            -- The NAMES of the sealed values, plain: the editor's
            -- connection list shows what a connection stores without
            -- the store opening every row.
            value_names JSONB NOT NULL DEFAULT '[]',
            granted_scopes JSONB NOT NULL DEFAULT '[]',
            -- Whether granted_scopes came from the provider (verified)
            -- or from the user's ticks (claimed); a shortfall only
            -- hard-fails when verified.
            permissions_verified BOOLEAN NOT NULL DEFAULT FALSE,
            -- Whose credential the row resolves to: 'their-own' (the
            -- stored values) or 'ours' (the runtime's credential
            -- source answers per call; values_sealed holds an empty map).
            owner TEXT NOT NULL DEFAULT 'their-own',
            -- Which door created it ('shared' / 'own'); drives the
            -- shared-door displacement warning.
            door TEXT NOT NULL DEFAULT 'own',
            -- The connection list's middle column: the app's label, or
            -- the name the user typed for a pasted credential.
            label TEXT,
            identity TEXT,
            -- The provider's own identifier for the account this
            -- connection belongs to (a workspace id, a mailbox
            -- address), captured at connect from the value the
            -- service's events recipe names. Indexed because an
            -- inbound event names this and nothing else: routing a
            -- push must be one index lookup, never a scan digging
            -- through every row's stored values. NULL for a service
            -- that reports no events.
            provider_account TEXT,
            -- The content hash (sha256 hex) of the spec snapshot's
            -- events block, NULL when it declares none. Written
            -- beside every spec_json write. An inbound push is
            -- answered by the recipe the connection itself declared,
            -- so routing filters on this against the verifying
            -- recipe's hash.
            events_recipe_hash TEXT,
            -- Set ONLY on a connection a node published for something
            -- it runs itself (`ctx.publish_access`): the id of that
            -- node. NULL on every connection a person made, which is
            -- what tells the two apart. A published connection is
            -- owned by its node: republishing finds it instead of
            -- making a second one, and terminating the node deletes
            -- it, so it lives exactly as long as the thing it opens.
            published_by_node TEXT,
            -- Whose connection, inside a project: NULL for one the
            -- author made (the project's, or a tenant-wide shared one),
            -- else the instance of `project_id` that connected it (or whose
            -- copy of a node published it). An instance is always a
            -- project's, so an instance's connection always names one.
            instance_id TEXT,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- How many times the connection was made again over this row
            -- (a reconnect or a rotate, `flows.rs`): what tells a signal
            -- acting as it that its credentials changed. A token refresh,
            -- a scope widened or a label renamed leaves it as it is.
            reconnects BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT access_grant_instance_has_project
                CHECK (instance_id IS NULL OR project_id IS NOT NULL)
        );
        CREATE INDEX IF NOT EXISTS access_grant_tenant_service
            ON access_grant (tenant_id, service);
        -- A node publishes ONE connection per service it runs, so
        -- republishing is a lookup on this key, and two racing runs
        -- cannot leave two rows behind.
        CREATE UNIQUE INDEX IF NOT EXISTS access_grant_published
            ON access_grant (tenant_id, project_id, published_by_node, service, instance_id) NULLS NOT DISTINCT
            WHERE published_by_node IS NOT NULL;
        -- An instance's connections, for its picker and its forget.
        CREATE INDEX IF NOT EXISTS access_grant_instance
            ON access_grant (project_id, instance_id) WHERE instance_id IS NOT NULL;
        -- What an instance provides for the fields its program writes
        -- `@instance_filled`: the value a run for it puts where the
        -- source would hold one. The node is its place, spelled the
        -- way the program reads it (`read`, `one.read`), and the field
        -- one of its inputs. A value that is a connection (the
        -- `{id, identity}` handle an access field holds) names its
        -- grant, so removing the connection removes every value using
        -- it, and its identity is read fresh from the grant.
        CREATE TABLE IF NOT EXISTS instance_value (
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            instance_id TEXT NOT NULL,
            node TEXT NOT NULL,
            field TEXT NOT NULL,
            value JSONB NOT NULL,
            grant_id UUID REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, instance_id, node, field)
        );
        CREATE INDEX IF NOT EXISTS instance_value_grant ON instance_value (grant_id) WHERE grant_id IS NOT NULL;
        -- The connection each of a program's access nodes uses on this
        -- install (`weft_core::picks`): picked here, never written in the
        -- source, since a connection's id means nothing on another
        -- install. One of the author's own connections (no instance's),
        -- and removing the connection removes the pick with it.
        CREATE TABLE IF NOT EXISTS install_pick (
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            node TEXT NOT NULL,
            field TEXT NOT NULL,
            grant_id UUID NOT NULL REFERENCES access_grant(id) ON DELETE CASCADE,
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (project_id, node, field)
        );
        CREATE INDEX IF NOT EXISTS install_pick_grant ON install_pick (grant_id);
        -- The inbound-event lookup: an incoming push names a service
        -- and an account, and must find every connection to it
        -- without knowing a tenant (which is the point: the push
        -- proves which account it concerns, and that IS the routing).
        CREATE INDEX IF NOT EXISTS access_grant_service_account
            ON access_grant (service, provider_account);
        -- One in-flight OAuth connect per state nonce. Postgres-backed so
        -- the callback may land on any dispatcher replica.
        CREATE TABLE IF NOT EXISTS access_connect (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- The resolved app credentials for the code exchange, carried
            -- from begin to callback (any dispatcher replica completes it).
            -- SEALED (crypt.rs).
            registration_sealed TEXT NOT NULL,
            project_id UUID,
            spec_json JSONB NOT NULL,
            scopes JSONB NOT NULL DEFAULT '[]',
            -- SEALED (crypt.rs): with the consent code intercepted, the
            -- verifier is what stands between a dump and the token.
            pkce_verifier TEXT,
            -- Which door started this consent ('shared' / 'own');
            -- recorded onto the grant at completion.
            door TEXT NOT NULL DEFAULT 'own',
            -- Set when this connect upgrades/rotates an existing
            -- exclusive-class grant in place.
            upgrade_grant_id UUID,
            -- The instance this connect is for (its browser went
            -- through an instance token, or the program's backend named it):
            -- recorded onto the grant at completion. NULL for the
            -- author's own connect.
            instance_id TEXT,
            redirect_uri TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        -- The sweep deletes abandoned rows by age; without this index
        -- it would scan.
        CREATE INDEX IF NOT EXISTS access_connect_created
            ON access_connect (created_at);
        -- One in-flight resource-picker session per state nonce: the
        -- node-declared chooser (script + glue) plus the connection it
        -- picks against. The weft-served picker page reads it; the
        -- outcome parks in access_connect_result like a consent's.
        CREATE TABLE IF NOT EXISTS access_picker (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            access_id UUID NOT NULL,
            service TEXT NOT NULL,
            script TEXT NOT NULL,
            code TEXT NOT NULL,
            mime_types JSONB NOT NULL DEFAULT '[]',
            -- The permissions choosing a resource GRANTS (the node's
            -- declared `grants`), parked with the session; a finished
            -- pick unions them into the grant row's granted_scopes.
            grants JSONB NOT NULL DEFAULT '[]',
            -- The instance the pick is for, when it opened at its own
            -- door: the chooser then signs in with a connection only
            -- that instance may use.
            project_id UUID,
            instance_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_picker_created
            ON access_picker (created_at);
        -- The browser flows' outcome (a consent's grant, a picker's
        -- pick), parked for the EDITOR to poll (the flow finished in a
        -- page the editor cannot see). One row per state nonce;
        -- consumed on read.
        CREATE TABLE IF NOT EXISTS access_connect_result (
            state TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            result_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS access_connect_result_created
            ON access_connect_result (created_at);
        -- One provider-side event subscription serving one registered
        -- signal: the id + token weft minted, what the provider
        -- answered (its resource id, the expiry), and which signal it
        -- feeds. Written when the serving side runs the service's
        -- subscribe call, renewed by re-running it before expires_at,
        -- deleted (after the provider's unsubscribe call) when the
        -- signal unregisters. An inbound push that routes by a minted
        -- id looks this table up and nothing else.
        CREATE TABLE IF NOT EXISTS signal_subscription (
            -- The id weft minted and sent to the provider.
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            -- Which event topic of the service this subscription is
            -- on; keys the recipe its calls and verification use.
            topic TEXT NOT NULL,
            access_id UUID NOT NULL,
            -- The registered signal this subscription feeds.
            signal_token TEXT NOT NULL,
            -- The secret weft minted; what a no-signature provider
            -- echoes on every push and the verify compares against.
            -- SEALED (crypt.rs): plain, a dump could forge pushes.
            token_sealed TEXT NOT NULL,
            -- Values the provider's subscribe answer captured
            -- (resource id, anything the unsubscribe call needs).
            captures_json JSONB NOT NULL DEFAULT '{}',
            expires_at TIMESTAMPTZ,
            -- The instance whose signal this is, when the connection is
            -- theirs: stopping the channel signs in as them.
            project_id UUID,
            instance_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        -- The events recipes seen per service, keyed by content hash:
        -- upserted from every spec that passes through a store flow
        -- (door probe, connects), read by the public events receiver,
        -- which must know how to verify and route a push BEFORE it
        -- knows which connection it concerns (and must answer a
        -- provider's address-proving handshake when no connection
        -- exists yet). Hash-keyed so a push is answered by the recipe
        -- the connection itself declared: routing pairs a verifying
        -- row's hash with access_grant.events_recipe_hash.
        CREATE TABLE IF NOT EXISTS service_events_recipe (
            service TEXT NOT NULL,
            -- sha256 hex of the recipe's canonical JSON.
            recipe_hash TEXT NOT NULL,
            events_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (service, recipe_hash)
        );
        -- The renewal sweep asks "which subscriptions die soon".
        CREATE INDEX IF NOT EXISTS signal_subscription_expiry
            ON signal_subscription (expires_at);
        -- Unregistering a signal deletes its subscriptions.
        CREATE INDEX IF NOT EXISTS signal_subscription_signal
            ON signal_subscription (signal_token);
CREATE OR REPLACE FUNCTION access_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP <> 'INSERT' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF OLD.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || OLD.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', OLD.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', OLD.project_id::text);
                    END IF;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF NEW.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || NEW.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', NEW.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS access_grant_notify ON access_grant;
CREATE TRIGGER access_grant_notify
            AFTER INSERT OR UPDATE OR DELETE ON access_grant
            FOR EACH ROW EXECUTE FUNCTION access_notify();
DROP TRIGGER IF EXISTS install_pick_notify ON install_pick;
CREATE TRIGGER install_pick_notify
            AFTER INSERT OR UPDATE OR DELETE ON install_pick
            FOR EACH ROW EXECUTE FUNCTION access_notify();
DROP TRIGGER IF EXISTS instance_value_notify ON instance_value;
CREATE TRIGGER instance_value_notify
            AFTER INSERT OR UPDATE OR DELETE ON instance_value
            FOR EACH ROW EXECUTE FUNCTION access_notify();
