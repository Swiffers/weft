ALTER TABLE run ADD CONSTRAINT run_program_identity_whole CHECK (((definition_hash IS NULL) = (binary_hash IS NULL)));
