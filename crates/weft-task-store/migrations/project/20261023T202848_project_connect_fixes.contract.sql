ALTER TABLE project ALTER COLUMN project_json_version DROP DEFAULT;

ALTER TABLE project ALTER COLUMN resync_version DROP DEFAULT;

ALTER TABLE project ALTER COLUMN running_build_inputs_version DROP DEFAULT;

ALTER TABLE project_definition ALTER COLUMN project_json_version DROP DEFAULT;
