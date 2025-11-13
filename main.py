import pandas as pd

from etl.extract import *
from etl.transform import *
from etl.load import load_to_dw
from connection import connect
from utils.model_loader import ModelRegistry

SCHEMA = 'dw' # Cambiar a None si no hay schema

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

def main():

    print("Iniciando prefase de precarga")
    co_oltp, etl_conn, _ = connect()

    # Cargamos los modelos de traduccion antes del ETL para evitar que interrumpan su flujo
    model_registry = ModelRegistry()
    model_registry.preload_model('en', 'es')
    model_registry.preload_model('en', 'fr')
    model_registry.preload_model('en', 'jap')
    model_registry.preload_model('en', 'de')
    model_registry.preload_model('en', 'trk')

    print("Iniciando proceso ETL")
    # Proceso DimSalesTerritory (independiente)
    df_sales_territory_raw = extract_sales_territory(co_oltp, 'sales')
    df_sales_territory_final = transform_sales_territory(df_sales_territory_raw)
    load_to_dw(df_sales_territory_final,'dim_sales_territory', SCHEMA, etl_conn)

    # Proceso DimCurrency (independiente)
    df_currency_raw = extract_currency(co_oltp)
    df_currency_final = transform_currency(df_currency_raw)
    load_to_dw(df_currency_final, 'dim_currency', SCHEMA, etl_conn)

    # Proceso DimGeography (depende de DimSalesTerritory)
    df_geography_raw = extract_geography(co_oltp)
    df_geography_final = transform_geography(df_geography_raw, etl_conn, model_registry)
    load_to_dw(df_geography_final, 'dim_geography', SCHEMA, etl_conn)

    # Proceso DimDate (independiente)
    df_date_final = transform_date()
    load_to_dw(df_date_final, 'dim_date', SCHEMA, etl_conn)

    # Proceso DimPromotion (independiente)
    df_promotion_raw = extract_promotion(co_oltp, 'sales')
    df_promotion_final = transform_promotion(df_promotion_raw, model_registry)
    load_to_dw(df_promotion_final, 'dim_promotion', SCHEMA, etl_conn)

    # Proceso DimProductCategory (independiente)
    df_product_category_raw = extract_product_category(co_oltp, 'production')
    df_product_category_final = transform_product_category(df_product_category_raw, model_registry)
    load_to_dw(df_product_category_final, 'dim_product_category', SCHEMA, etl_conn)

    # Proceso DimProductSubCategory (depende de DimProductCategory)
    df_product_subcategory_raw = extract_product_subcategory(co_oltp, 'production')
    df_product_subcategory_final = transform_product_subcategory(df_product_subcategory_raw, etl_conn,model_registry)
    load_to_dw(df_product_subcategory_final, 'dim_product_subcategory', SCHEMA, etl_conn)

    # Proceso DimProduct (depende de DimProductSubCategory)
    df_product_raw = extract_product(co_oltp, 'production')
    df_sales_order = extract_sales_order(co_oltp)
    df_product_model = extract_product_model(co_oltp)
    df_large_photo = extract_large_photo(co_oltp)
    df_product_price_list = extract_product_price_list(co_oltp)
    df_language_description_raw = extract_language_description(co_oltp)
    df_pivoted_language_description = transform_language_description(df_language_description_raw)
    df_product_raw_final = transform_product(
        df_product_raw,
        df_sales_order,
        df_product_model,
        df_large_photo,
        df_product_price_list,
        df_pivoted_language_description,
        etl_conn,
        model_registry
    )
    load_to_dw(df_product_raw_final, 'dim_product', SCHEMA, etl_conn)

    # Proceso DimCustomer (depende de DimGeography)
    df_customer_raw = extract_customer(co_oltp)
    df_customer_final = transforms_customer(df_customer_raw, etl_conn, model_registry)
    load_to_dw(df_customer_final, 'dim_customer', SCHEMA, etl_conn)

    # Proceso FactInternetSales (depende de DimCustomer, DimProduct, DimPromotion, DimDate, DimCurrency y DimSalesTerritory)
    df_fact_internet_sales_raw = extract_fact_internet_sales(co_oltp)
    df_special_offer = extract_special_offer(co_oltp)
    df_fact_internet_sales_final = transforms_fact_internet_sales(
        df_fact_internet_sales_raw,
        df_special_offer,
        etl_conn
    )
    load_to_dw(df_fact_internet_sales_final, 'fact_internet_sales', SCHEMA, etl_conn)

if __name__ == "__main__":
    main()