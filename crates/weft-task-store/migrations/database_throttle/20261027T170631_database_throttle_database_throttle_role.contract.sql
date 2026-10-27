ALTER TABLE database_throttle ADD CONSTRAINT database_throttle_role_check CHECK ((role = ANY (ARRAY['machine'::text, 'dispatcher'::text, 'broker'::text])));
