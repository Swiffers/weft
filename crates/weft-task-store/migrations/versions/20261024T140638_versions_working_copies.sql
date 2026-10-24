CREATE TABLE working_copy (
    conflicts text[] NOT NULL,
    created_at bigint NOT NULL,
    head_run uuid,
    head_version text,
    host text,
    id uuid NOT NULL,
    kind text NOT NULL,
    path text,
    project_id uuid NOT NULL,
    revision bigint NOT NULL,
    synced jsonb NOT NULL,
    updated_at bigint NOT NULL,
    working_manifest jsonb NOT NULL
);

ALTER TABLE working_copy ADD CONSTRAINT working_copy_check CHECK (((kind = 'folder'::text) = ((host IS NOT NULL) AND (path IS NOT NULL))));

ALTER TABLE working_copy ADD CONSTRAINT working_copy_kind_check CHECK ((kind = ANY (ARRAY['folder'::text, 'program'::text])));

ALTER TABLE working_copy ADD CONSTRAINT working_copy_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX idx_working_copy_folder ON working_copy USING btree (project_id, host, path) WHERE (kind = 'folder'::text);

CREATE UNIQUE INDEX idx_working_copy_program ON working_copy USING btree (project_id) WHERE (kind = 'program'::text);

CREATE INDEX idx_working_copy_project ON working_copy USING btree (project_id);
