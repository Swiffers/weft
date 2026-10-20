ALTER TABLE runtime_file ADD COLUMN receives text;

ALTER TABLE runtime_file ADD COLUMN slot_accepts text;

CREATE INDEX idx_runtime_file_receives ON runtime_file USING btree (receives) WHERE (receives IS NOT NULL);
