-- Creating the database and schema
create or replace database TPCDS;

create or replace schema RAW;

-- Creating inventory table
create or replace table TPCDS.RAW.inventory (
inv_date_sk int not null,
inv_item_sk int not null,
inv_quantity_on_hand int,
inv_warehouse_sk int not null
);

-- Creating a user and password
create or replace user analytical_user
password = 'analyticalUser123';

-- Granting the use and admin role
grant role accountadmin to user analytical_user;
