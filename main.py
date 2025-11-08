import pandas as pd

from etl.extract import extract_sales_territory, extract_currency, extract_geography
from etl.load import load_to_dw
from etl.transform import transform_sales_territory, transform_currency, transform_geography
from connection import connect
from utils.model_loader import ModelRegistry

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

def main():

    print("Iniciando prefase de precarga")
    co_oltp, etl_conn, _ = connect()

    model_registry = ModelRegistry()
    model_registry.preload_model('en', 'es')
    model_registry.preload_model('en', 'fr')

    print("Iniciando proceso ETL")
    # Proceso DimSalesTerritory (independiente)
    df_sales_territory_raw = extract_sales_territory(co_oltp, 'sales')
    df_sales_territory_final = transform_sales_territory(df_sales_territory_raw)
    load_to_dw(df_sales_territory_final,'dim_sales_territory','dw',etl_conn)

    # Proceso DimCurrency (independiente)
    df_currency_raw = extract_currency(co_oltp)
    df_currency_final = transform_currency(df_currency_raw)
    load_to_dw(df_currency_final, 'dim_currency', 'dw', etl_conn)

    # Proceso DimGeography (depende de DimSalesTerritory)
    df_geography_raw = extract_geography(co_oltp)
    df_geography_final = transform_geography(df_geography_raw, etl_conn, model_registry)
    load_to_dw(df_geography_final, 'dim_geography', 'dw', etl_conn)


if __name__ == "__main__":
    main()