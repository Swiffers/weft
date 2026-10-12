ALTER TABLE access_grant ADD COLUMN reconnects bigint DEFAULT 0 NOT NULL;

ALTER TABLE access_grant ADD CONSTRAINT access_grant_reconnects_not_null NOT NULL reconnects;
