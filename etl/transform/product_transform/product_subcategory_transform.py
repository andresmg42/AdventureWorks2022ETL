import pandas as pd
from sqlalchemy import Engine
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

def transform_product_subcategory(df_subcategory_base: pd.DataFrame, df_dim_product_category: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando product subcategory")
    df_subcategory_base = df_subcategory_base.rename(columns={'product_subcategory_id': 'product_subcategory_alternate_key', 'name': 'english_product_subcategory_name'})
    df_subcategory_base=df_subcategory_base.drop(['rowguid', 'modified_date'], axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_subcategory_base = convert_language('english_product_subcategory_name', 'spanish_product_subcategory_name', tokenizer_es, model_es, df_subcategory_base)
    df_subcategory_base = convert_language('english_product_subcategory_name', 'french_product_subcategory_name', tokenizer_fr, model_fr, df_subcategory_base)

    df_subcategory_base = df_subcategory_base.merge(
        df_dim_product_category[['product_category_alternate_key', 'product_category_key']],
        left_on='product_category_id',
        right_on='product_category_alternate_key',
        how='left'
    )

    df_subcategory_base = df_subcategory_base.drop(['product_category_alternate_key', 'product_category_id'], axis=1)

    return df_subcategory_base