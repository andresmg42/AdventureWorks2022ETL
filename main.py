import pandas as pd
from etl.etl_pipeline import *
from etl.transform.readers import read_sql, load_dimension
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

    print("Inicializando clase ETLContext")
    ctx = ETLContext(
        oltp_engine=co_oltp,
        dw_engine=etl_conn,
        dest_schema=SCHEMA,
        model_registry=model_registry
    )

    print("Iniciando proceso ETL")
    sales_territory_pipeline(ctx)
    ctx.dim_cache["dim_sales_territory"] = pd.read_sql(read_sql('dim_sales_territory_keys'), ctx.dw_engine)

    currency_pipeline(ctx)
    ctx.dim_cache["dim_currency"] = load_dimension('dim_currency', ctx.dw_engine, SCHEMA)

    geography_pipeline(ctx)
    ctx.dim_cache['dim_geography'] = pd.read_sql(read_sql('dim_geography_keys'), ctx.dw_engine)

    date_pipeline(ctx)
    promotion_pipeline(ctx)

    product_category_pipeline(ctx)
    ctx.dim_cache["dim_product_category"] = load_dimension('dim_product_category', ctx.dw_engine, SCHEMA)

    product_subcategory_pipeline(ctx)
    ctx.dim_cache['dim_product_subcategory'] = pd.read_sql(read_sql('dim_product_subcategory'), ctx.dw_engine)

    product_pipeline(ctx)

    customer_pipeline(ctx)
    employee_pipeline(ctx)
    reseller_pipeline(ctx)

    ctx.dim_cache["dim_product"] = load_dimension('dim_product', ctx.dw_engine, SCHEMA)
    ctx.dim_cache["dim_promotion"] = load_dimension('dim_promotion', ctx.dw_engine, SCHEMA)
    ctx.dim_cache["dim_customer"] = load_dimension('dim_customer', ctx.dw_engine, SCHEMA)
    ctx.dim_cache["dim_employee"] = load_dimension('dim_employee', ctx.dw_engine, SCHEMA)
    ctx.dim_cache["dim_reseller"] = load_dimension('dim_reseller', ctx.dw_engine, SCHEMA)

    fact_internet_sales_pipeline(ctx)
    fact_reseller_sales_pipeline(ctx)

if __name__ == "__main__":
    main()