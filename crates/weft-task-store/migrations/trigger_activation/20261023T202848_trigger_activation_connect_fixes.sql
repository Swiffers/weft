-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE trigger_activation ADD COLUMN activation_program_version integer DEFAULT 1 NOT NULL;
