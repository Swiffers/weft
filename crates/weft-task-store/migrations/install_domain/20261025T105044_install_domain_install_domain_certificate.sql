ALTER TABLE install_domain ADD COLUMN certificate text DEFAULT 'pending'::text NOT NULL;

ALTER TABLE install_domain ADD COLUMN certificate_why text;

CREATE OR REPLACE FUNCTION install_domain_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_install_domains', CASE TG_OP WHEN 'UPDATE' THEN 'certificate' ELSE '' END);
                RETURN NULL;
            END;
            $function$
;

DROP TRIGGER install_domain_changed ON install_domain;
CREATE TRIGGER install_domain_changed AFTER INSERT OR DELETE OR UPDATE OF certificate, certificate_why ON install_domain FOR EACH ROW EXECUTE FUNCTION install_domain_notify();
