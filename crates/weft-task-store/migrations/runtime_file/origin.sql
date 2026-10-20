-- One row per runtime file. `key` is the canonical
        -- `<tenant>/<scope>/<owner>/<id>` string (also the bucket object key
        -- under the `runtime/` prefix). `tenant_id` is the first key segment,
        -- denormalized so per-tenant usage + listing are indexed lookups.
        CREATE TABLE IF NOT EXISTS runtime_file (
            key                TEXT PRIMARY KEY,
            tenant_id          TEXT NOT NULL,
            mime_type          TEXT NOT NULL,
            filename           TEXT NOT NULL,
            size_bytes         BIGINT NOT NULL,
            -- Upload lifecycle. 'pending': the row was reserved at upload begin,
            -- BEFORE any bytes; it carries the multipart resume state below and
            -- its reserved_bytes are already charged against the tenant's byte
            -- quota. 'active': the upload completed (bytes assembled + sized).
            -- The row exists FIRST and bytes land SECOND, so the bucket never
            -- holds an object with no row; a 'pending' row whose upload never
            -- completed is reaped by the row-driven sweeps (which also abort its
            -- multipart upload). 'completing': a completion claimed the
            -- upload (its parts are final) and is having the bucket assemble
            -- the object; nothing may abort it, change its parts, or remove
            -- the file it replaces until it lands, and progressed_at_unix
            -- holds the claim time, from which the tenant's next upload drives
            -- a completion its caller abandoned. 'reaping': a call fenced the row for removal
            -- (writers and reads are locked out; the bucket state goes next,
            -- then the row), and progressed_at_unix holds the claim time; a
            -- reap that crashed or failed leaves the row in 'reaping', and
            -- once its claim lapsed (or a failed reap gave it back) the
            -- expiry loop, a wipe or the tenant's next upload takes it
            -- over. Only 'active' rows
            -- appear in user listings / gets; ALL statuses count toward the
            -- byte quota ('active' by size_bytes, others by reserved_bytes),
            -- which is what makes an in-flight upload unable to blow past the
            -- cap.
            status             TEXT NOT NULL DEFAULT 'active',
            -- True iff this run-scoped file is flagged to survive the
            -- terminate sweep. Always false in the other scopes, which
            -- outlive runs without a flag. Set at begin; a PENDING kept
            -- row is still sweepable (only kept ACTIVE files are spared).
            keep               BOOLEAN NOT NULL DEFAULT FALSE,
            -- Unix seconds at which the file expires (access-bumped), in
            -- any scope: a kept run file, a project/shared/instance
            -- file stored with a lifetime, a retired asset. NULL = no
            -- expiry. Set at complete, never on a pending row.
            expires_at_unix    BIGINT,
            -- The file's TTL so an access can recompute expiry. NULL when
            -- there is no expiry.
            keep_ttl_secs      BIGINT,
            created_at_unix    BIGINT NOT NULL,
            -- Multipart upload state, present on a 'pending' row (NULL once
            -- active). upload_id is the bucket's multipart handle (the resume
            -- handle); part_size is the fixed size of every non-final part.
            upload_id          TEXT,
            part_size          BIGINT,
            -- The total size declared at begin, NULL for an unknown-length
            -- stream. A declared upload's parts must slice exactly to it.
            declared_size      BIGINT,
            -- The bytes CHARGED against the tenant quota for this in-flight
            -- upload: the declared total (known size) or the running sum of
            -- reserved parts (stream). Every reserved part's exact size is
            -- signed into its URL, so the bucket enforces this number.
            reserved_bytes     BIGINT NOT NULL DEFAULT 0,
            -- Progress clock for the abandoned-pending reap: bumped whenever a
            -- part is reserved, so a long multi-part upload that is still
            -- moving is never reaped mid-flight. On a 'completing' or
            -- 'reaping' row, the time of its claim; 0 on a 'reaping' row
            -- whose reap failed, which gave its claim back.
            progressed_at_unix BIGINT NOT NULL DEFAULT 0,
            -- What the file is a copy OF, when the writer said (a message
            -- id, a document id at a provider): a begin naming an identity
            -- the scope already holds answers that file instead of minting
            -- another. NULL for a file that is its own thing.
            identity           TEXT,
            -- On a 'pending' row only: the key of the ACTIVE file this
            -- upload will overwrite (`StorageHandle::replace`). The upload
            -- writes straight to that file's object, which the bucket swaps
            -- in whole when the multipart completes, so readers see the old
            -- bytes until then and the new ones after. Completion folds the
            -- new size into that file's row and deletes this one. NULL for
            -- an upload that makes a new file.
            replaces           TEXT,
            -- On an 'active' row only: the key of the last replacement
            -- upload folded into this file (that upload's own row is gone
            -- once folded). A retried complete of that upload finds the
            -- file here and answers with it. NULL until a replacement lands.
            last_replacement   TEXT,
            -- Which write of this file's content the row describes: 1 when
            -- the file is made, and one more at the end of every
            -- replacement (folded, aborted or reaped alike, since the
            -- bytes may have been swapped either way). A key is never
            -- reused, so (key, version) names one content. Every stored
            -- file value carries it; a replacement that names the version
            -- it read is refused when the file has moved on since.
            version            BIGINT NOT NULL DEFAULT 1,
            -- On a 'reaping' row only: why a reap of it failed. While any
            -- row is marked, a failure naming them is written down for
            -- `weft status`. A failed reap gives its claim back: the expiry
            -- loop's next pass, a wipe or the tenant's next upload takes it
            -- again, and no look is booked for it until one does. The mark
            -- stays while that reap runs, so its record clears only once
            -- the row is gone.
            reap_error         TEXT
        );
        -- One replacement in flight per file: its multipart writes onto
        -- the file's own object, so a second one would race it there.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_one_replacement
            ON runtime_file(replaces) WHERE replaces IS NOT NULL;
        -- A replacement folds into exactly one file, and a key is never
        -- reused: the lookup a retried complete makes.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_last_replacement
            ON runtime_file(last_replacement) WHERE last_replacement IS NOT NULL;
        -- One file per identity per scope (the key minus its id): the
        -- lookup begin makes, and the guarantee that two runs fetching the
        -- same thing at once cannot both land.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_identity
            ON runtime_file((regexp_replace(key, '/[^/]+$', '')), identity)
            WHERE identity IS NOT NULL;
        -- One row per RESERVED part of a pending upload: the exact size signed
        -- into its URL, and the etag once the caller reports it landed (NULL =
        -- reserved but not yet landed, i.e. what resume re-presigns). Rows are
        -- deleted at complete; ON DELETE CASCADE ties them to the file row for
        -- every sweep/abort path.
        CREATE TABLE IF NOT EXISTS runtime_file_part (
            key           TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            part_number   INT NOT NULL,
            size_bytes    BIGINT NOT NULL,
            etag          TEXT,
            PRIMARY KEY (key, part_number)
        );
        -- Per-tenant usage + listing range over the key prefix; the index on
        -- (tenant_id, key) serves the tenant-usage sum and the prefix list.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_tenant ON runtime_file(tenant_id);
        -- The expiry loop ranges files by their expiry.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_expiry
            ON runtime_file(expires_at_unix) WHERE expires_at_unix IS NOT NULL;
        -- One row per minted PUBLIC RELAY link: the public
        -- `/public/files/{token}` route resolves the token here and
        -- streams the file. `fetch_url` is a presigned GET, signed for the
        -- endpoint the runtime does its own I/O on (the broker relays the
        -- bytes from it), for the same lifetime
        -- as the token. Rows expire with the link; every mint deletes
        -- the expired ones, so the table stays the size of the live
        -- link set. ON DELETE CASCADE ties a link to its file row, so a
        -- deleted/swept file takes its links with it and a live token
        -- can never point at bytes that are gone (a dead token is a
        -- clean 404, never a broken stream).
        CREATE TABLE IF NOT EXISTS public_file_link (
            token           TEXT PRIMARY KEY,
            key             TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            mime_type       TEXT NOT NULL,
            filename        TEXT NOT NULL,
            fetch_url       TEXT NOT NULL,
            expires_at_unix BIGINT NOT NULL
        );
        -- Which projects reference which of the tenant's assets
        -- (`<tenant>/asset/<sha256>`): one row per (project, asset), the
        -- project's whole set replaced at every publish. An asset is one
        -- file per tenant whichever projects hold its content, so it lives
        -- while ANY project of the tenant has a row for it (no expiry), and
        -- the publish that removes its last row starts its countdown. No
        -- foreign key to `runtime_file`: a row records what the project's
        -- sources and versions name, which stays true when the file itself
        -- is gone (the next publish reports it missing).
        CREATE TABLE IF NOT EXISTS asset_reference (
            tenant_id   TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            key         TEXT NOT NULL,
            PRIMARY KEY (tenant_id, project_id, key)
        );
        -- "Does any project still reference this asset?": the question a
        -- publish asks of every asset it stops referencing.
        CREATE INDEX IF NOT EXISTS idx_asset_reference_key ON asset_reference(key);
