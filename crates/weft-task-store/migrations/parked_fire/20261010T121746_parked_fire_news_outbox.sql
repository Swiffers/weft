CREATE OR REPLACE FUNCTION parked_fire_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_project UUID;
            BEGIN
                IF NEW.execution_id IS NULL THEN
                    PERFORM weft_announce('weft_parked_fire', (SELECT s.project_id FROM signal s WHERE s.token = NEW.token)::text);
                ELSE
                    SELECT r.project_id INTO v_project FROM run r WHERE r.execution_id = NEW.execution_id;
                    PERFORM weft_announce('weft_parked_fire', v_project::text || ' ' || NEW.execution_id::text);
                    PERFORM weft_tell(v_project, NEW.execution_id, 'answer');
                END IF;
                RETURN NULL;
            END;
            $function$
;
