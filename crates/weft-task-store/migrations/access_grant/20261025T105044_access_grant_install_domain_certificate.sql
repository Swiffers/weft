CREATE TABLE access_transfer_key (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    id uuid NOT NULL,
    secret_sealed text NOT NULL,
    tenant_id text NOT NULL
);

ALTER TABLE access_transfer_key ADD CONSTRAINT access_transfer_key_pkey PRIMARY KEY (id);
