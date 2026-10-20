ALTER TABLE infra_lifecycle_command ADD COLUMN waits_on_tokens text[] DEFAULT '{}'::text[] NOT NULL;

ALTER TABLE infra_lifecycle_command ADD CONSTRAINT infra_lifecycle_command_waits_on_tokens_not_null NOT NULL waits_on_tokens;
