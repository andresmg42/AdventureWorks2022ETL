CREATE USER andres WITH PASSWORD '12345';
GRANT ALL PRIVILEGES ON DATABASE adventure_works_dw TO andres;

SET ROLE andres;
CREATE SCHEMA dw;

CREATE TABLE IF NOT EXISTS dw.dim_currency (
    currency_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    currency_alternate_key CHAR(3) NOT NULL,
    currency_name VARCHAR(50) NOT NULL,
    CONSTRAINT dim_currency_pkey PRIMARY KEY (currency_key)
  );

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    geography_key INT NULL,
    customer_alternate_key VARCHAR(15) NOT NULL,
    title VARCHAR(8) NULL,
    first_name VARCHAR(50) NULL,
    middle_name VARCHAR(50) NULL,
    last_name VARCHAR(50) NULL,
    name_style BOOLEAN NULL,
    birth_date DATE NULL,
    marital_status CHAR(1) NULL,
    suffix VARCHAR(10) NULL,
    gender VARCHAR(1) NULL,
    email_address VARCHAR(50) NULL,
    yearly_income NUMERIC(19, 4) NULL,
    total_children SMALLINT NULL,
    number_children_at_home SMALLINT NULL,
    english_education VARCHAR(40) NULL,
    spanish_education VARCHAR(40) NULL,
    french_education VARCHAR(40) NULL,
    english_occupation VARCHAR(100) NULL,
    spanish_occupation VARCHAR(100) NULL,
    french_occupation VARCHAR(100) NULL,
    house_owner_flag CHAR(1) NULL,
    number_cars_owned SMALLINT NULL,
    address_line1 VARCHAR(120) NULL,
    address_line2 VARCHAR(120) NULL,
    phone VARCHAR(20) NULL,
    date_first_purchase DATE NULL,
    commute_distance VARCHAR(15) NULL,
    CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INT NOT NULL,
    full_date_alternate_key DATE NOT NULL,
    day_number_of_week SMALLINT NOT NULL,
    english_day_name_of_week VARCHAR(10) NOT NULL,
    spanish_day_name_of_week VARCHAR(10) NOT NULL,
    french_day_name_of_week VARCHAR(10) NOT NULL,
    day_number_of_month SMALLINT NOT NULL,
    day_number_of_year SMALLINT NOT NULL,
    week_number_of_year SMALLINT NOT NULL,
    english_month_name VARCHAR(10) NOT NULL,
    spanish_month_name VARCHAR(10) NOT NULL,
    french_month_name VARCHAR(10) NOT NULL,
    month_number_of_year SMALLINT NOT NULL,
    calendar_quarter SMALLINT NOT NULL,
    calendar_year SMALLINT NOT NULL,
    calendar_semester SMALLINT NOT NULL,
    fiscal_quarter SMALLINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    fiscal_semester SMALLINT NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
  );

CREATE TABLE IF NOT EXISTS dw.dim_employee (
    employee_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    employee_national_id_alternate_key VARCHAR(15) NULL,
    sales_territory_key INT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    middle_name VARCHAR(50) NULL,
    name_style SMALLINT NOT NULL,
    title VARCHAR(50) NULL,
    hire_date DATE NULL,
    birth_date DATE NULL,
    login_id VARCHAR(256) NULL,
    email_address VARCHAR(50) NULL,
    phone VARCHAR(25) NULL,
    marital_status CHAR(1) NULL,
    emergency_contact_name VARCHAR(50) NULL,
    emergency_contact_phone VARCHAR(25) NULL,
    salaried_flag SMALLINT NULL,
    gender CHAR(1) NULL,
    pay_frequency SMALLINT NULL,
    base_rate NUMERIC(19, 4) NULL,
    vacation_hours SMALLINT NULL,
    sick_leave_hours SMALLINT NULL,
    current_flag SMALLINT NOT NULL,
    sales_person_flag SMALLINT NOT NULL,
    department_name VARCHAR(50) NULL,
    start_date DATE NULL,
    end_date DATE NULL,
    status VARCHAR(50) NULL,
    employee_photo BYTEA NULL,
    CONSTRAINT dim_employee_pkey PRIMARY KEY (employee_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_geography (
    geography_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    city VARCHAR(30) NULL,
    state_province_code VARCHAR(3) NULL,
    state_province_name VARCHAR(50) NULL,
    country_region_code VARCHAR(3) NULL,
    english_country_region_name VARCHAR(50) NULL,
    spanish_country_region_name VARCHAR(50) NULL,
    french_country_region_name VARCHAR(50) NULL,
    postal_code VARCHAR(15) NULL,
    sales_territory_key INT NULL,
    CONSTRAINT dim_geography_pkey PRIMARY KEY (geography_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_alternate_key VARCHAR(25) NULL,
    product_subcategory_key INT NULL,
    weight_unit_measure_code CHAR(3) NULL,
    size_unit_measure_code CHAR(3) NULL,
    english_product_name VARCHAR(50) NOT NULL,
    spanish_product_name VARCHAR(50) NOT NULL,
    french_product_name VARCHAR(50) NOT NULL,
    standard_cost NUMERIC(19, 4) NULL,
    finished_goods_flag BOOLEAN NOT NULL,
    color VARCHAR(15) NOT NULL,
    safety_stock_level SMALLINT NULL,
    reorder_point SMALLINT NULL,
    list_price NUMERIC(19, 4) NULL,
    size VARCHAR(50) NULL,
    size_range VARCHAR(50) NULL,
    weight FLOAT8 NULL,
    days_to_manufacture INT NULL,
    product_line CHAR(2) NULL,
    dealer_price NUMERIC(19, 4) NULL,
    class CHAR(2) NULL,
    style CHAR(2) NULL,
    model_name VARCHAR(50) NULL,
    large_photo BYTEA NULL,
    english_description VARCHAR(400) NULL,
    french_description VARCHAR(400) NULL,
    chinese_description VARCHAR(400) NULL,
    arabic_description VARCHAR(400) NULL,
    hebrew_description VARCHAR(400) NULL,
    thai_description VARCHAR(400) NULL,
    german_description VARCHAR(400) NULL,
    japanese_description VARCHAR(400) NULL,
    turkish_description VARCHAR(400) NULL,
    start_date TIMESTAMP(3) NULL,
    end_date TIMESTAMP(3) NULL,
    status VARCHAR(7) NULL,
    CONSTRAINT dim_product_pkey PRIMARY KEY (product_key),
    CONSTRAINT dim_product_product_alternate_key_start_date_key UNIQUE (product_alternate_key, start_date)
);

CREATE TABLE IF NOT EXISTS dw.dim_product_category (
    product_category_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_category_alternate_key INT NULL,
    english_product_category_name VARCHAR(50) NOT NULL,
    spanish_product_category_name VARCHAR(50) NOT NULL,
    french_product_category_name VARCHAR(50) NOT NULL,
    CONSTRAINT dim_product_category_pkey PRIMARY KEY (product_category_key),
    CONSTRAINT dim_product_category_product_category_alternate_key_key UNIQUE (product_category_alternate_key)
  );

CREATE TABLE IF NOT EXISTS dw.dim_product_subcategory (
    product_subcategory_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_subcategory_alternate_key INT NULL,
    english_product_subcategory_name VARCHAR(50) NOT NULL,
    spanish_product_subcategory_name VARCHAR(50) NOT NULL,
    french_product_subcategory_name VARCHAR(50) NOT NULL,
    product_category_key INT NULL,
    CONSTRAINT dim_product_subcategory_pkey PRIMARY KEY (product_subcategory_key),
    CONSTRAINT dim_product_subcategory_product_subcategory_alternate_key_key UNIQUE (product_subcategory_alternate_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_promotion (
    promotion_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    promotion_alternate_key INT NULL,
    english_promotion_name VARCHAR(255) NULL,
    spanish_promotion_name VARCHAR(255) NULL,
    french_promotion_name VARCHAR(255) NULL,
    discount_pct FLOAT8 NULL,
    english_promotion_type VARCHAR(50) NULL,
    spanish_promotion_type VARCHAR(50) NULL,
    french_promotion_type VARCHAR(50) NULL,
    english_promotion_category VARCHAR(50) NULL,
    spanish_promotion_category VARCHAR(50) NULL,
    french_promotion_category VARCHAR(50) NULL,
    start_date TIMESTAMP(3) NOT NULL,
    end_date TIMESTAMP(3) NULL,
    min_qty INT NULL,
    max_qty INT NULL,
    CONSTRAINT dim_promotion_pkey PRIMARY KEY (promotion_key),
    CONSTRAINT dim_promotion_promotion_alternate_key_key UNIQUE (promotion_alternate_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_reseller (
    reseller_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    geography_key INT NULL,
    reseller_alternate_key VARCHAR(15) NULL,
    phone VARCHAR(25) NULL,
    business_type VARCHAR(20) NOT NULL,
    reseller_name VARCHAR(50) NOT NULL,
    number_employees INT NULL,
    order_frequency CHAR(1) NULL,
    order_month SMALLINT NULL,
    first_order_year INT NULL,
    last_order_year INT NULL,
    product_line VARCHAR(50) NULL,
    address_line1 VARCHAR(60) NULL,
    address_line2 VARCHAR(60) NULL,
    annual_sales NUMERIC(19, 4) NULL,
    bank_name VARCHAR(50) NULL,
    min_payment_type SMALLINT NULL,
    min_payment_amount NUMERIC(19, 4) NULL,
    annual_revenue NUMERIC(19, 4) NULL,
    year_opened INT NULL,
    CONSTRAINT dim_reseller_pkey PRIMARY KEY (reseller_key),
    CONSTRAINT dim_reseller_reseller_alternate_key_key UNIQUE (reseller_alternate_key)
);

CREATE TABLE IF NOT EXISTS dw.dim_sales_territory (
    sales_territory_key INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    sales_territory_alternate_key INT NULL,
    sales_territory_region VARCHAR(50) NOT NULL,
    sales_territory_country VARCHAR(50) NOT NULL,
    sales_territory_group VARCHAR(50) NULL,
    sales_territory_image BYTEA NULL,
    CONSTRAINT dim_sales_territory_pkey PRIMARY KEY (sales_territory_key),
    CONSTRAINT dim_sales_territory_sales_territory_alternate_key_key UNIQUE (sales_territory_alternate_key)
);

CREATE TABLE IF NOT EXISTS dw.fact_internet_sales (
    product_key INT NOT NULL,
    order_date_key INT NOT NULL,
    due_date_key INT NOT NULL,
    ship_date_key INT NOT NULL,
    customer_key INT NOT NULL,
    promotion_key INT NOT NULL,
    currency_key INT NOT NULL,
    sales_territory_key INT NOT NULL,
    sales_order_number VARCHAR(20) NOT NULL,
    sales_order_line_number SMALLINT NOT NULL,
    revision_number SMALLINT NOT NULL,
    order_quantity SMALLINT NOT NULL,
    unit_price NUMERIC(19, 4) NOT NULL,
    extended_amount NUMERIC(19, 4) NOT NULL,
    unit_price_discount_pct FLOAT8 NOT NULL,
    discount_amount FLOAT8 NOT NULL,
    product_standard_cost NUMERIC(19, 4) NOT NULL,
    total_product_cost NUMERIC(19, 4) NOT NULL,
    sales_amount NUMERIC(19, 4) NOT NULL,
    tax_amt NUMERIC(19, 4) NOT NULL,
    freight NUMERIC(19, 4) NOT NULL,
    carrier_tracking_number VARCHAR(25) NULL,
    customer_po_number VARCHAR(25) NULL,
    order_date TIMESTAMP(3) NULL,
    due_date TIMESTAMP(3) NULL,
    ship_date TIMESTAMP(3) NULL,
    CONSTRAINT fact_internet_sales_pkey PRIMARY KEY (sales_order_number, sales_order_line_number)
);

CREATE TABLE IF NOT EXISTS dw.fact_reseller_sales (
    product_key INT NOT NULL,
    order_date_key INT NOT NULL,
    due_date_key INT NOT NULL,
    ship_date_key INT NOT NULL,
    reseller_key INT NOT NULL,
    employee_key INT NOT NULL,
    promotion_key INT NOT NULL,
    currency_key INT NOT NULL,
    sales_territory_key INT NOT NULL,
    sales_order_number VARCHAR(20) NOT NULL,
    sales_order_line_number SMALLINT NOT NULL,
    revision_number SMALLINT NULL,
    order_quantity SMALLINT NULL,
    unit_price NUMERIC(19, 4) NULL,
    extended_amount NUMERIC(19, 4) NULL,
    unit_price_discount_pct FLOAT8 NULL,
    discount_amount FLOAT8 NULL,
    product_standard_cost NUMERIC(19, 4) NULL,
    total_product_cost NUMERIC(19, 4) NULL,
    sales_amount NUMERIC(19, 4) NULL,
    tax_amt NUMERIC(19, 4) NULL,
    freight NUMERIC(19, 4) NULL,
    carrier_tracking_number VARCHAR(25) NULL,
    customer_po_number VARCHAR(25) NULL,
    order_date TIMESTAMP(3) NULL,
    due_date TIMESTAMP(3) NULL,
    ship_date TIMESTAMP(3) NULL,
    CONSTRAINT fact_reseller_sales_pkey PRIMARY KEY (sales_order_number, sales_order_line_number)
);

ALTER TABLE dw.dim_customer ADD CONSTRAINT dim_customer_geography_key_fkey FOREIGN KEY(geography_key) REFERENCES dw.dim_geography (geography_key);
ALTER TABLE dw.dim_employee ADD CONSTRAINT dim_employee_sales_territory_key_fkey FOREIGN KEY(sales_territory_key) REFERENCES dw.dim_sales_territory (sales_territory_key);
ALTER TABLE dw.dim_geography ADD CONSTRAINT dim_geography_sales_territory_key_fkey FOREIGN KEY(sales_territory_key) REFERENCES dw.dim_sales_territory (sales_territory_key);
ALTER TABLE dw.dim_product ADD CONSTRAINT dim_product_product_subcategory_key_fkey FOREIGN KEY(product_subcategory_key) REFERENCES dw.dim_product_subcategory (product_subcategory_key);
ALTER TABLE dw.dim_product_subcategory ADD CONSTRAINT dim_product_subcategory_product_category_key_fkey FOREIGN KEY(product_category_key) REFERENCES dw.dim_product_category (product_category_key);
ALTER TABLE dw.dim_reseller ADD CONSTRAINT dim_reseller_geography_key_fkey FOREIGN KEY(geography_key) REFERENCES dw.dim_geography (geography_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_currency_key_fkey FOREIGN KEY(currency_key) REFERENCES dw.dim_currency (currency_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_customer_key_fkey FOREIGN KEY(customer_key) REFERENCES dw.dim_customer (customer_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_order_date_key_fkey FOREIGN KEY(order_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_due_date_key_fkey FOREIGN KEY(due_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_ship_date_key_fkey FOREIGN KEY(ship_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_product_key_fkey FOREIGN KEY(product_key) REFERENCES dw.dim_product (product_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_promotion_key_fkey FOREIGN KEY(promotion_key) REFERENCES dw.dim_promotion (promotion_key);
ALTER TABLE dw.fact_internet_sales ADD CONSTRAINT fact_internet_sales_sales_territory_key_fkey FOREIGN KEY(sales_territory_key) REFERENCES dw.dim_sales_territory (sales_territory_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_currency_key_fkey FOREIGN KEY(currency_key) REFERENCES dw.dim_currency (currency_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_order_date_key_fkey FOREIGN KEY(order_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_due_date_key_fkey FOREIGN KEY(due_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_ship_date_key_fkey FOREIGN KEY(ship_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_employee_key_fkey FOREIGN KEY(employee_key) REFERENCES dw.dim_employee (employee_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_product_key_fkey FOREIGN KEY(product_key) REFERENCES dw.dim_product (product_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_promotion_key_fkey FOREIGN KEY(promotion_key) REFERENCES dw.dim_promotion (promotion_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_reseller_key_fkey FOREIGN KEY(reseller_key) REFERENCES dw.dim_reseller (reseller_key);
ALTER TABLE dw.fact_reseller_sales ADD CONSTRAINT fact_reseller_sales_sales_territory_key_fkey FOREIGN KEY(sales_territory_key) REFERENCES dw.dim_sales_territory (sales_territory_key);