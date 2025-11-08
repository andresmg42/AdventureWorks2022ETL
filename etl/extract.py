import pandas as pd
from sqlalchemy.engine import Engine

def extract_sales_territory(co_olt: Engine, schema) -> pd.DataFrame:
    print("EXTRACT: Leyendo SalesTerritory")

    return pd.read_sql_table('sales_territory',co_olt, schema)

def extract_currency(co_olt: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo Currency")

    df_currency = pd.read_sql(
        'SELECT * FROM sales.currency ORDER BY name ASC;',
        co_olt)

    return df_currency