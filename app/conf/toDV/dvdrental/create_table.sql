
--LINK

CREATE TABLE dvdrental.film_actor (
	gen_actor_id int8 NULL,
	gen_film_id int8 NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.film_category (
	gen_film_id int8 NULL,
	gen_category_id int8 NULL,
	last_update timestamp NULL
);



--Incremental

CREATE TABLE dvdrental.payment (
	gen_payment_id int8 NULL,
	source_payment_id int8 NULL,
	customer_id int8 NULL,
	staff_id int8 NULL,
	rental_id int8 NULL,
	amount float8 NULL,
	payment_date timestamp NULL
);

CREATE TABLE dvdrental.rental (
	gen_rental_id int8 NULL,
	source_rental_id int8 NULL,
	rental_date timestamp NULL,
	inventory_id int8 NULL,
	customer_id int8 NULL,
	return_date timestamp NULL,
	staff_id int8 NULL,
	last_update timestamp NULL
);


--SCDTYPE1


CREATE TABLE dvdrental.film (
	gen_film_id int8 NULL,
	source_film_id int8 NULL,
	title text NULL,
	description text NULL,
	release_year int8 NULL,
	language_id int8 NULL,
	rental_duration int8 NULL,
	rental_rate float8 NULL,
	length int8 NULL,
	replacement_cost float8 NULL,
	rating text NULL,
	last_update timestamp NULL,
	special_features text NULL,
	fulltext text NULL
);


CREATE TABLE dvdrental.actor (
	gen_actor_id int8 NULL,
	source_actor_id int8 NULL,
	first_name text NULL,
	last_name text NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.city (
	gen_city_id int8 NULL,
	source_city_id int8 NULL,
	city text NULL,
	country_id int8 NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.country (
	gen_country_id int8 NULL,
	source_country_id int8 NULL,
	country text NULL,
	last_update timestamp NULL
);


CREATE TABLE dvdrental."language" (
	gen_language_id int8 NULL,
	source_language_id int8 NULL,
	"name" text NULL,
	last_update timestamp NULL
);

CREATE TABLE dvdrental.category (
	gen_category_id int8 NULL,
	source_category_id int8 NULL,
	"name" text NULL,
	last_update timestamp NULL
);



--SCDTYPE2


CREATE TABLE dvdrental.store (
	gen_store_id int8 NULL,
	source_store_id int8 null,
	start_ts timestamp(6),
	end_ts timestamp(6),
	manager_staff_id int8 NULL,
	address_id int8 NULL
);



CREATE TABLE dvdrental.staff (
	gen_staff_id int8 NULL,
	source_staff_id int8 NULL,
	start_ts timestamp(6),
	end_ts timestamp(6),
	first_name text NULL,
	last_name text NULL,
	address_id int8 NULL,
	email text NULL,
	store_id int8 NULL,
	active bool NULL,
	username text NULL,
	"password" text NULL,
	picture text NULL
);




CREATE TABLE dvdrental.customer (
	gen_customer_id int8 NULL,
	source_customer_id int8 NULL,
	start_ts timestamp(6),
	end_ts timestamp(6),
	store_id int8 NULL,
	first_name text NULL,
	last_name text NULL,
	email text NULL,
	address_id int8 NULL,
	activebool bool NULL,
	create_date date NULL,
	active int8 NULL
);


CREATE TABLE dvdrental.inventory (
	gen_inventory_id int8 NULL,
	source_inventory_id int8 NULL,
	start_ts timestamp(6),
	end_ts timestamp(6),
	film_id int8 NULL,
	store_id int8 NULL
);



CREATE TABLE dvdrental.address (
	gen_address_id int8 NULL,
	source_address_id int8 NULL,
	start_ts timestamp(6),
	end_ts timestamp(6),
	address text NULL,
	address2 text NULL,
	district text NULL,
	city_id int8 NULL,
	postal_code text NULL,
	phone text NULL
);
