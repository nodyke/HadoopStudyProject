CREATE TABLE public.top_product_in_each_cat
(
    category character varying COLLATE pg_catalog."default" NOT NULL,
    product_name character varying COLLATE pg_catalog."default" NOT NULL,
    cnt bigint,
    CONSTRAINT top_product_in_each_cat_pkey PRIMARY KEY (category, product_name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.top_product_in_each_cat
    OWNER to dbocharov;