import pandas as pd
from sqlalchemy import Engine
from etl.utils_etl import get_sales_territory_image

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