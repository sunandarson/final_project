DROP TABLE IF EXISTS dim_ct;
CREATE TABLE dim_ct(
	id_transaction BIGINT NULL,
	id_customer BIGINT NULL,
    name_customer VARCHAR(255),
	birthdate_customer DATE,
	gender_customer varchar(255),
	country_customer varchar(255),
    date_transaction DATE,
	product VARCHAR(255),
	product_transaction VARCHAR(255),
	amount_transaction BIGINT NULL
);