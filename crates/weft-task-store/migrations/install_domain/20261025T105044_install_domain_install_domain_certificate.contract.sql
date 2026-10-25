ALTER TABLE install_domain ADD CONSTRAINT install_domain_certificate_check CHECK ((certificate = ANY (ARRAY['pending'::text, 'issued'::text, 'failed'::text])));
