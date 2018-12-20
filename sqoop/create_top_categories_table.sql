CREATE TABLE public.top_categories
(
    category character varying COLLATE pg_catalog."default" NOT NULL,
    count bigint NOT NULL,
    CONSTRAINT top_categories_pkey PRIMARY KEY (category)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.top_categories
    OWNER to dbocharov;