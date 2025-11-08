import pandas as pd
from sqlalchemy import text, Engine
from etl.utils_etl import get_sales_territory_image
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

def transform_sales_territory(df: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando salesTerritory")
    df['sales_territory_country'] = df['name']

    df['sales_territory_country'] = df.apply(
        lambda row: 'United States' if row['country_region_code'] == 'US' else row['sales_territory_country'],
        axis=1)
    df.drop(
        ['country_region_code', 'sales_ytd', 'sales_last_year', 'cost_ytd', 'cost_last_year', 'rowguid', 'modified_date'],
        axis=1,
        inplace=True)

    df.rename(
        columns={'name': 'sales_territory_region',
                 'territory_id': 'sales_territory_alternate_key',
                 'group_name': 'sales_territory_group'
                 },
        inplace=True)

    df['sales_territory_image'] = df.apply(get_sales_territory_image, axis=1)

    return df

def transform_currency(df: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando currency")
    df.rename(columns={'currency_code': 'currency_alternate_key', 'name': 'currency_name'}, inplace=True)
    df.drop(['modified_date'],axis=1,inplace=True)

    return df

def transform_geography(df: pd.DataFrame, etl_conn: Engine, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando geography")

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df.rename(columns={'country_region_name':'english_country_region_name'},inplace=True)

    df = convert_language('english_country_region_name', 'spanish_country_region_name', tokenizer_es, model_es, df)
    df = convert_language('english_country_region_name', 'french_country_region_name', tokenizer_fr, model_fr, df)

    df_sales_territory_keys = pd.read_sql(
        text("""
             SELECT sales_territory_key, sales_territory_alternate_key
             FROM dw.dim_sales_territory
             """),
        etl_conn
    )

    df_geo_linked = pd.merge(
        df,
        df_sales_territory_keys,
        on='sales_territory_alternate_key',
        how='left'
    )

    columns_to_load = [
        'city', 'state_province_code', 'state_province_name',
        'country_region_code', 'english_country_region_name', 'spanish_country_region_name',
        'french_country_region_name', 'postal_code', 'sales_territory_key'
    ]

    return df_geo_linked[columns_to_load]

