ALTER TABLE infra_unit ADD CONSTRAINT infra_unit_check CHECK (((last_restart_at_unix IS NULL) = (last_restart_cause IS NULL)));

ALTER TABLE infra_unit ADD CONSTRAINT infra_unit_check1 CHECK (((last_restart_exit_code IS NOT NULL) = (NOT (last_restart_cause IS DISTINCT FROM 'exited'::text))));

ALTER TABLE infra_unit ADD CONSTRAINT infra_unit_last_restart_cause_check CHECK ((last_restart_cause = ANY (ARRAY['out_of_memory'::text, 'exited'::text, 'unknown'::text])));
