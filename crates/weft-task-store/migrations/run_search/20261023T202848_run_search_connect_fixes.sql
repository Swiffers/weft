ALTER TABLE run_search ADD COLUMN format integer DEFAULT 1 NOT NULL;

CREATE INDEX run_search_format ON run_search USING btree (format);
