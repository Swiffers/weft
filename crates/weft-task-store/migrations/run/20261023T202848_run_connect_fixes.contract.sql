ALTER TABLE run ALTER COLUMN spec_version DROP DEFAULT;

ALTER TABLE run_log ALTER COLUMN events_version DROP DEFAULT;

ALTER TABLE signal ALTER COLUMN program_json_version DROP DEFAULT;

ALTER TABLE signal ALTER COLUMN spec_json_version DROP DEFAULT;

ALTER TABLE trigger_bake ALTER COLUMN bake_json_version DROP DEFAULT;
