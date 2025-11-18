import pandas as pd
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language

def transform_promotion(df_promotion_base: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando promotion")
    df_promotion_base = df_promotion_base.rename(columns={
        'special_offer_id': 'promotion_alternate_key',
        'description': 'english_promotion_name',
        'type': 'english_promotion_type',
        'category': 'english_promotion_category',
    })

    df_promotion_base = df_promotion_base.drop(['rowguid', 'modified_date'], axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_promotion_base=convert_language('english_promotion_name', 'french_promotion_name', tokenizer_fr, model_fr, df_promotion_base)
    df_promotion_base=convert_language('english_promotion_name', 'spanish_promotion_name', tokenizer_es, model_es, df_promotion_base)
    df_promotion_base=convert_language('english_promotion_category', 'french_promotion_category', tokenizer_fr, model_fr, df_promotion_base)
    df_promotion_base=convert_language('english_promotion_category', 'spanish_promotion_category', tokenizer_es, model_es, df_promotion_base)
    df_promotion_base=convert_language('english_promotion_type', 'french_promotion_type', tokenizer_fr, model_fr, df_promotion_base)
    df_promotion_base=convert_language('english_promotion_type', 'spanish_promotion_type', tokenizer_es, model_es, df_promotion_base)

    return df_promotion_base