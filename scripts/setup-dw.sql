CREATE USER andres WITH PASSWORD '12345';
GRANT ALL PRIVILEGES ON DATABASE adventure_works_dw TO andres;

SET ROLE andres;
CREATE SCHEMA dw;

CREATE TABLE IF NOT EXISTS dw.dim_geography (
        geography_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        city TEXT,
        postal_code TEXT,
        state_province_code TEXT,
        state_province_name TEXT,
        country_region_code TEXT,
		country_region_name TEXT,
        spanish_country_region_name TEXT,
        sales_territory_key INT,
        modified_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
	customer_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_alternate_key TEXT,
    geography_key INT,
    title TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    suffix TEXT,
    name_style BOOLEAN,
    birth_date DATE,
    gender VARCHAR(1),
    marital_status VARCHAR(1),
    yearly_income TEXT,
    total_children INT,
    number_children_at_home INT,
    education TEXT,
    spanish_education TEXT,
    occupation TEXT,
    spanish_occupation TEXT,
    home_owner_flag BOOLEAN,
    number_cars_owned INT,
    email_address TEXT,
    phone_number TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    date_first_purchase DATE,
    commute_distance TEXT
 );