ALTER TABLE access_grant ALTER COLUMN spec_json_version DROP DEFAULT;

ALTER TABLE service_events_recipe ALTER COLUMN events_json_version DROP DEFAULT;
