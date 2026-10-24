-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE access_grant ADD COLUMN spec_json_version integer DEFAULT 1 NOT NULL;

ALTER TABLE access_grant ADD COLUMN token_lifetime_secs bigint;

-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE service_events_recipe ADD COLUMN events_json_version integer DEFAULT 1 NOT NULL;
