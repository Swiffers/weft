ALTER TABLE project_frontend ADD COLUMN visitor_address text;

CREATE UNIQUE INDEX idx_project_frontend_visitor_address ON project_frontend USING btree (project_id) WHERE (visitor_address IS NOT NULL);
