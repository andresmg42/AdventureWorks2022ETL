import pandas as pd
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

def transform_product_category(df_category_base: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando product category")
    df_category_base = df_category_base.rename(columns={'product_category_id': 'product_category_alternate_key', 'name': 'english_product_category_name'})
    df_category_base = df_category_base.drop(['rowguid', 'modified_date'], axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_category_base = convert_language('english_product_category_name', 'spanish_product_category_name', tokenizer_es, model_es, df_category_base)
    df_category_base = convert_language('english_product_category_name', 'french_product_category_name', tokenizer_fr, model_fr, df_category_base)

    return df_category_base
