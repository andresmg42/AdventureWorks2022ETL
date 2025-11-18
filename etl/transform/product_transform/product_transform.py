import pandas as pd
import numpy as np
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

def transform_product(
        df_product_base: pd.DataFrame,
        df_sales_order: pd.DataFrame,
        df_product_model: pd.DataFrame,
        df_large_photo: pd.DataFrame,
        df_product_price_list: pd.DataFrame,
        df_pivoted_descriptions: pd.DataFrame,
        df_product_subcategory: pd.DataFrame,
        model_registry: ModelRegistry
) -> pd.DataFrame:
    print("TRANSFORM: Transformando product")
    df_product = (
        df_product_base
        .rename(columns={'product_number': 'product_alternate_key', 'name': 'english_product_name'})
        .merge(
            df_product_subcategory[['product_subcategory_alternate_key', 'product_subcategory_key']],
            left_on='product_subcategory_id',
            right_on='product_subcategory_alternate_key',
            how='left'
        )
        .drop(columns=['product_subcategory_alternate_key', 'product_subcategory_id'])
        .merge(df_sales_order[['product_id', 'dealer_price']], on='product_id', how='left')
        .merge(df_product_model, on='product_model_id', how='left')
        .rename(columns={'name': 'model_name'}) # Renombramos después del merge para evitar conflictos
        .merge(df_large_photo, on='product_id', how='left')
        .merge(df_product_price_list, on='product_id', how='left')
        .merge(df_pivoted_descriptions, on='product_id', how='left')
    )

    size_numeric = pd.to_numeric(df_product['size'], errors='coerce')

    conditions = [
        size_numeric.between(38, 40, inclusive='both'),
        size_numeric.between(42, 46, inclusive='both'),
        size_numeric.between(48, 52, inclusive='both'),
        size_numeric.between(54, 58, inclusive='both'),
        size_numeric.between(60, 62, inclusive='both'),
    ]

    choices = [
        '38–40 CM',
        '42–46 CM',
        '48–52 CM',
        '54–58 CM',
        '60–62 CM',
    ]

    df_product['size_range'] = np.select(conditions, choices, default='NA')

    df_product['status'] = np.where(df_product['end_date'].isna(), 'Current', None)
    df_product['color'] = df_product['color'].fillna('NA')


    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')
    tokenizer_jap, model_jap = model_registry.get_model('en', 'jap')
    tokenizer_de, model_de = model_registry.get_model('en', 'de')
    tokenizer_trk, model_trk = model_registry.get_model('en', 'trk')

    df_product = convert_language('english_product_name', 'french_product_name', tokenizer_fr, model_fr, df_product)
    df_product = convert_language('english_product_name', 'spanish_product_name', tokenizer_es, model_es, df_product)
    df_product = convert_language('english_description', 'japanese_description', tokenizer_jap, model_jap, df_product)
    df_product = convert_language('english_description', 'german_description', tokenizer_de, model_de, df_product)
    df_product = convert_language('english_description', 'turkish_description', tokenizer_trk, model_trk, df_product)

    df_product=df_product.drop(['product_id', 'make_flag', 'product_model_id', 'discontinued_date', 'rowguid', 'modified_date', 'sell_start_date', 'sell_end_date'], axis=1)

    df_product = df_product.replace({'nan': None})

    return df_product