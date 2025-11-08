import pandas as pd

from etl.extract import extract_sales_territory, extract_currency
from etl.load import load_to_dw
from etl.transform import transform_sales_territory, transform_currency
from connection import connect

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

def main():

    print("Iniciando proceso ETL")
    co_oltp, etl_conn, _ = connect()

    # Proceso DimSalesTerritory (independiente)
    df_sales_territory_raw = extract_sales_territory(co_oltp, 'sales')
    df_sales_territory_final = transform_sales_territory(df_sales_territory_raw)
    load_to_dw(df_sales_territory_final,'dim_sales_territory','dw',etl_conn)

    # Proceso DimCurrency (independiente)
    df_currency_raw = extract_currency(co_oltp)
    df_currency_final = transform_currency(df_currency_raw)
    load_to_dw(df_currency_final, 'dim_currency', 'dw', etl_conn)

    # Proceso DimGeography (depende de DimSalesTerritory)

if __name__ == "__main__":
    main()