import pandas as pd
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

columns_to_load = [
    'city', 'state_province_code', 'state_province_name',
    'country_region_code', 'english_country_region_name', 'spanish_country_region_name',
    'french_country_region_name', 'postal_code', 'sales_territory_key'
]

def transform_geography(df_geography_base: pd.DataFrame, df_dim_sales_territory: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando geography")

    df_geography_base = df_geography_base.rename(columns={'country_region_name': 'english_country_region_name'})

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_geography_base = convert_language('english_country_region_name', 'spanish_country_region_name', tokenizer_es, model_es, df_geography_base)
    df_geography_base = convert_language('english_country_region_name', 'french_country_region_name', tokenizer_fr, model_fr, df_geography_base)

    df_geography_base = df_geography_base.merge(
        df_dim_sales_territory,
        on='sales_territory_alternate_key',
        how='left'
    )

    return df_geography_base[columns_to_load]