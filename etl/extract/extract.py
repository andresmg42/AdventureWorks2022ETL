import pandas as pd
from sqlalchemy import Engine
from etl.extract.utils_extract import _read_table
from etl.extract.sql_reader import read_sql

def extract_sales_territory(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'sales_territory')

def extract_currency(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo Currency")
    return pd.read_sql(read_sql('currency'), co_oltp)

def extract_geography(co_olt: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo Geography")
    return pd.read_sql(read_sql('geography'), co_olt)

def extract_promotion(co_otl: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_otl, schema, 'special_offer')

def extract_product_category(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product_category')

def extract_product_subcategory(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product_subcategory')

def extract_sales_order(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo sales order")
    return pd.read_sql(read_sql('sales_order'), co_oltp)


def extract_product_model(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo product model")
    return pd.read_sql(read_sql('product_model'), co_oltp)

def extract_large_photo(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo large photo")
    return pd.read_sql(read_sql('large_photo'), co_oltp)


def extract_product_price_list(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo price list")
    return pd.read_sql(read_sql('product_price_list'), co_oltp)

def extract_language_description(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo language description")
    return pd.read_sql(read_sql('language_description'), co_oltp)

def extract_product(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product')

def extract_customer(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo customer")
    return pd.read_sql(read_sql('customer'), co_oltp)

def extract_emergency_contact_data(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo contact table")
    return pd.read_sql(read_sql('emergency_contact'), co_oltp)

def extract_sales_person(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo sales person")
    return pd.read_sql(read_sql('sales_person'), co_oltp)


def extract_pay_frequency(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo pay frequency")
    return pd.read_sql(read_sql('pay_frequency'), co_oltp)


def extract_base_rate(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo base rate")
    return pd.read_sql(read_sql('base_rate'), co_oltp)

def extract_employee(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo employee")
    return pd.read_sql(read_sql('employee'), co_oltp)

def extract_reseller(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo reseller")
    return pd.read_sql(read_sql('reseller'), co_oltp)


def extract_special_offer_internet_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo special offer para internet sales")
    return pd.read_sql(read_sql('special_offer_internet_sales'), co_oltp)

def extract_fact_internet_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo fact internet sales")
    return pd.read_sql(read_sql('fact_internet_sales'), co_oltp)

def extract_special_offer_reseller_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo special offer para reseller sales")
    return pd.read_sql(read_sql('special_offer_reseller_sales'), co_oltp)

def extract_currency_reseller_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo currency para reseller sales")
    return pd.read_sql(read_sql('currency_reseller_sales'), co_oltp)

def extract_fact_reseller_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo fact reseller sales")
    return pd.read_sql(read_sql('fact_reseller_sales'), co_oltp)
