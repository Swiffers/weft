ALTER TABLE runtime_file ADD COLUMN content_sha256 text;

ALTER TABLE runtime_file ADD COLUMN links_until_unix bigint;

ALTER TABLE runtime_file ADD COLUMN slot_received_by text;

ALTER TABLE runtime_file ADD COLUMN upload_slot text;

CREATE INDEX idx_runtime_file_content ON runtime_file USING btree (tenant_id, content_sha256) WHERE (content_sha256 IS NOT NULL);

CREATE UNIQUE INDEX idx_runtime_file_upload_slot ON runtime_file USING btree (upload_slot) WHERE ((upload_slot IS NOT NULL) AND (status <> 'reaping'::text));

-- Throws away every row in public_file_link.
DROP TABLE public_file_link;
