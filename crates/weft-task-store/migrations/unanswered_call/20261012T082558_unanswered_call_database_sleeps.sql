CREATE TABLE unanswered_alert (
    after_ms bigint NOT NULL,
    one boolean DEFAULT true NOT NULL
);

ALTER TABLE unanswered_call ADD COLUMN alerted_ms bigint;

ALTER TABLE unanswered_call ADD COLUMN label text;

ALTER TABLE unanswered_alert ADD CONSTRAINT unanswered_alert_after_ms_not_null NOT NULL after_ms;

ALTER TABLE unanswered_alert ADD CONSTRAINT unanswered_alert_one_check CHECK (one);

ALTER TABLE unanswered_alert ADD CONSTRAINT unanswered_alert_one_not_null NOT NULL one;

ALTER TABLE unanswered_alert ADD CONSTRAINT unanswered_alert_pkey PRIMARY KEY (one);
