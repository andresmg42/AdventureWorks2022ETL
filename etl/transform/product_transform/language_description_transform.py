import pandas as pd

def transform_language_description(df_language_description: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando language description")
    columns = df_language_description['name'].unique().tolist()

    new_columns = {name: f'{name.lower()}_description' for name in columns}
    df_language_description = df_language_description.pivot(
        index='product_id', columns='name',
        values='description').reset_index()

    df_language_description = df_language_description.rename(columns=new_columns)

    return df_language_description
