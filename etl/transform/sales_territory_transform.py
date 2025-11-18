import pandas as pd
import numpy as np
from etl.transform.utils_transform import get_sales_territory_image

def transform_sales_territory(df_sales_territory_base: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando salesTerritory")
    df_sales_territory_base['sales_territory_country'] = df_sales_territory_base['name']

    df_sales_territory_base['sales_territory_country'] = np.where(
        df_sales_territory_base['country_region_code'] == 'US',
        'United States',
        df_sales_territory_base['sales_territory_country']
    )

    df_sales_territory_base = df_sales_territory_base.drop(
        ['country_region_code', 'sales_ytd', 'sales_last_year', 'cost_ytd', 'cost_last_year', 'rowguid', 'modified_date'],
        axis=1
    )

    df_sales_territory_base = df_sales_territory_base.rename(
        columns={'name': 'sales_territory_region',
                 'territory_id': 'sales_territory_alternate_key',
                 'group_name': 'sales_territory_group'
                 }
    )

    df_sales_territory_base['sales_territory_image'] = df_sales_territory_base.apply(get_sales_territory_image, axis=1)

    return df_sales_territory_base