CREATE TABLE public.top_country
(
    country_name character varying COLLATE pg_catalog."default" NOT NULL,
    s bigint,
    CONSTRAINT top_country_pkey PRIMARY KEY (country_name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.top_country
    OWNER to dbocharov;