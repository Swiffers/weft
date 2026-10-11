CREATE TABLE weft_news (
    execution_id uuid NOT NULL,
    kind text NOT NULL,
    project_id uuid NOT NULL,
    seq bigint GENERATED ALWAYS AS IDENTITY NOT NULL
);

ALTER TABLE weft_news ADD CONSTRAINT weft_news_execution_id_not_null NOT NULL execution_id;

ALTER TABLE weft_news ADD CONSTRAINT weft_news_kind_check CHECK ((kind = ANY (ARRAY['answer'::text, 'cancel'::text])));

ALTER TABLE weft_news ADD CONSTRAINT weft_news_kind_not_null NOT NULL kind;

ALTER TABLE weft_news ADD CONSTRAINT weft_news_pkey PRIMARY KEY (seq);

ALTER TABLE weft_news ADD CONSTRAINT weft_news_project_id_not_null NOT NULL project_id;

ALTER TABLE weft_news ADD CONSTRAINT weft_news_seq_not_null NOT NULL seq;

CREATE OR REPLACE FUNCTION weft_tell(p_project uuid, p_execution uuid, p_kind text)
 RETURNS void
 LANGUAGE sql
AS $function$
            INSERT INTO weft_news (project_id, execution_id, kind) VALUES (p_project, p_execution, p_kind)
            $function$
;
