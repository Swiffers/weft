-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE project ADD COLUMN project_json_version integer DEFAULT 1 NOT NULL;

ALTER TABLE project ADD COLUMN resync_driver uuid;

-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE project ADD COLUMN resync_version integer DEFAULT 1 NOT NULL;

ALTER TABLE project ADD COLUMN running_build_inputs jsonb;

-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE project ADD COLUMN running_build_inputs_version integer DEFAULT 1 NOT NULL;

-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE project_definition ADD COLUMN project_json_version integer DEFAULT 1 NOT NULL;
