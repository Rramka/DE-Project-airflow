
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

----create etl conf table for FULL
create table Etl_Process_Mapping (

  ID SERIAL,
  SourceTableName varchar,
  SourceDBName varchar,
  SourceSchema varchar,
  TableType varchar,
  DestDBName varchar,
  DestTableName varchar,
  DestSchema varchar,
  FilterColumn varchar,
  InsertionType varchar,
  NaturalKey varchar,
  SurogateKey varchar,
  Code int,
  Stage varchar,
  Last_Run_date Date
);

    insert into Etl_Process_Mapping (
      SourceTableName,
      SourceDBName,
      SourceSchema,
      TableType,
      DestDBName,
      DestTableName,
      DestSchema,
      InsertionType,
      Stage
     )
     values ('store',   'dvdrental',   'public',   'full',  'DBStaging',   'store',   'dvdrental',   'replace', 'SqlToStaging'),
        ('category',   'dvdrental',   'public',   'full',   'DBStaging',   'category',   'dvdrental',   'replace' , 'SqlToStaging' ),
        ('language',   'dvdrental',   'public',   'full',   'DBStaging',   'language',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('address',   'dvdrental',   'public',   'full',   'DBStaging',   'address',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('city',   'dvdrental',   'public',   'full',   'DBStaging',   'city',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('country',   'dvdrental',   'public',   'full',   'DBStaging',   'country',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('film',   'dvdrental',   'public',   'full',   'DBStaging',   'film',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('actor',   'dvdrental',   'public',   'full',   'DBStaging',   'actor',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('staff',   'dvdrental',   'public',   'full',   'DBStaging',   'staff',   'dvdrental',   'replace' , 'SqlToStaging'),
        ('inventory',   'dvdrental',   'public',   'full',   'DBStaging',   'inventory',   'dvdrental',   'replace' , 'SqlToStaging')

    
    
select * from etlconf.etl_process_mapping epm

update etl_process_mapping set stage=lower(stage)

-------create etl conf table for INCREMENTAL
    insert into Etl_Process_Mapping (
      SourceTableName,
      SourceDBName,
      SourceSchema,
      TableType,
      DestDBName,
      DestTableName,
      DestSchema,
      InsertionType,
      Stage,
      FilterColumn
     )
     values ('rental',   'dvdrental',   'public',   'INCREMENTAL',  'DBStaging',   'rental',   'dvdrental', 'append', 'SqlToStaging','rental_date'),
('payment',   'dvdrental',   'public',   'INCREMENTAL',  'DBStaging',   'payment',   'dvdrental', 'append', 'SqlToStaging','payment_date'),
('film_actor',   'dvdrental',   'public',   'INCREMENTAL',  'DBStaging',   'film_actor',   'dvdrental', 'append', 'SqlToStaging','last_update'),
('film_category',   'dvdrental',   'public',   'INCREMENTAL',  'DBStaging',   'film_category',   'dvdrental', 'append', 'SqlToStaging','last_update'),
('customer',   'dvdrental',   'public',   'INCREMENTAL',  'DBStaging',   'customer',   'dvdrental', 'append', 'SqlToStaging','last_update')

---------    insert for staging to dv stage (scdtype1)
insert into Etl_Process_Mapping (
  SourceTableName,
  SourceDBName,
  SourceSchema,
  TableType,
  InsertionType,
  DestDBName,
  DestTableName,
  DestSchema,
  NaturalKey,
  SurogateKey,
  Code,
  stage
     )
     values ('film', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'film', 'dvdrental', 'film_id', 'film_id', 1,'stagingtodv'),
     ('country', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'country', 'dvdrental', 'country_id', 'country_id', 1,'stagingtodv'),
     ('actor', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'actor', 'dvdrental', 'actor_id', 'actor_id', 1,'stagingtodv'),
     ('city', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'city', 'dvdrental', 'city_id', 'city_id', 1,'stagingtodv'),
     ('language', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'language', 'dvdrental', 'language_id', 'language_id', 1,'stagingtodv'),
     ('category', 'DBStaging', 'dvdrental', 'full', 'replace', 'DBDV', 'category', 'dvdrental', 'category_id', 'category_id', 1,'stagingtodv')


select * from Etl_Process_Mapping  where sourcedbname ='DBStaging'

update etl_process_mapping set stage = 'stagingtodv' where stage  is null


--------insert staging to dv incremental tables----------------------
 insert into postgres.etlconf.Etl_Process_Mapping  (
  SourceTableName,
  SourceDBName,
  SourceSchema,
  TableType,
  InsertionType,
  DestDBName,
  DestTableName,
  DestSchema,
  FilterColumn,
  NaturalKey,
  SurogateKey,
  Code,
  stage
     )
values ('rental',   'DBStaging',   'dvdrental',   'INCREMENTAL',  'append',   'DBDV',   'rental', 'dvdrental', 'rental_date','rental_id','rental_id',1,'stagingtodv'),
('payment',   'DBStaging',   'dvdrental',   'INCREMENTAL',  'append',   'DBDV',   'payment', 'dvdrental', 'payment_date','payment_id','payment_id',1,'stagingtodv')



--------- insert staging to dv Link tables-------------------------

 insert into postgres.etlconf.Etl_Process_Mapping  (
    SourceTableName,
  SourceDBName,
  SourceSchema,
  TableType,
  InsertionType,
  DestDBName,
  DestTableName,
  DestSchema,
  FilterColumn,
  SurogateKey,
  Code,
  stage
     )
values
('film_category',   'dbstaging',   'dvdrental',   'link',  'replace',   'dbdv',   'film_category', 'dvdrental', 'last_update',('film_id','category_id'),1,'stagingtodv')
('film_actor',   'dbstaging',   'dvdrental',   'link',  'replace',   'dbdv',   'film_actor', 'dvdrental', 'last_update',('film_id','actor_id'),1,'stagingtodv')



------------insert  staging to dv scdtype2 tables --------------------------

 insert into postgres.etlconf.Etl_Process_Mapping  (
  SourceTableName,
  SourceDBName,
  SourceSchema,
  TableType,
  InsertionType,
  DestDBName,
  DestTableName,
  DestSchema,
  NaturalKey,
  SurogateKey,
  FilterColumn,
  Code,
  stage
     )
values
('staff',   'dbstaging',   'dvdrental',   'scd',  'replace',   'dbdv',   'staff', 'dvdrental', 'staff_id','staff_id', 'last_update', 1,'stagingtodv'),
('customer', 'dbstaging',   'dvdrental',   'scd',  'replace',   'dbdv',   'customer', 'dvdrental', 'customer_id','customer_id','last_update',1,'stagingtodv'),
('inventory',   'dbstaging',   'dvdrental',   'scd',  'replace',   'dbdv',   'inventory', 'dvdrental',  'inventory_id','inventory_id','last_update',1,'stagingtodv'),
('address',   'dbstaging',   'dvdrental',   'scd',  'replace',   'dbdv',   'address', 'dvdrental', 'address_id','address_id','last_update',1,'stagingtodv'),
('store',   'dbstaging',   'dvdrental',   'scd',  'replace',   'dbdv',   'store', 'dvdrental', 'store_id','store_id','last_update',1,'stagingtodv')

