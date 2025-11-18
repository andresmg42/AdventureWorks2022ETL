import pandas as pd

def transform_currency(df_currency_base: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando currency")
    df_currency_base = df_currency_base.rename(columns={'currency_code': 'currency_alternate_key', 'name': 'currency_name'})
    df_currency_base = df_currency_base.drop(['modified_date'], axis=1)

    return df_currency_base