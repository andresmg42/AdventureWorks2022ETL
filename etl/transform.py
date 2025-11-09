import pandas as pd
from sqlalchemy import text, Engine
from etl.utils_etl import get_sales_territory_image
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language
from utils.days_and_moths import english_days, english_months, spanish_days, spanish_months, french_days, french_months

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

def transform_geography(df: pd.DataFrame, etl_conn: Engine, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando geography")

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df.rename(columns={'country_region_name':'english_country_region_name'},inplace=True)

    df = convert_language('english_country_region_name', 'spanish_country_region_name', tokenizer_es, model_es, df)
    df = convert_language('english_country_region_name', 'french_country_region_name', tokenizer_fr, model_fr, df)

    df_sales_territory_keys = pd.read_sql(
        text("""
             SELECT sales_territory_key, sales_territory_alternate_key
             FROM dw.dim_sales_territory
             """),
        etl_conn
    )

    df_geo_linked = pd.merge(
        df,
        df_sales_territory_keys,
        on='sales_territory_alternate_key',
        how='left'
    )

    columns_to_load = [
        'city', 'state_province_code', 'state_province_name',
        'country_region_code', 'english_country_region_name', 'spanish_country_region_name',
        'french_country_region_name', 'postal_code', 'sales_territory_key'
    ]

    return df_geo_linked[columns_to_load]

def transform_date() -> pd.DataFrame:
    print("TRANSFORM: Generando date")
    df_date = pd.DataFrame({
        "date": pd.date_range(start='1/1/2005', end='31/12/2014', freq='D')
    })

    df_date['day_number_of_week'] = ((df_date['date'].dt.day_of_week + 1) % 7) + 1
    df_date['day_number_of_month']=df_date['date'].dt.day
    df_date['day_number_of_year']=df_date['date'].dt.day_of_year
    df_date['week_number_of_year'] = df_date['date'].apply(
        lambda d: ((d - pd.Timestamp(d.year, 1, 1)).days + pd.Timestamp(d.year, 1, 1).weekday() + 1) // 7 + 1
    )
    df_date['month_number_of_year']=df_date['date'].dt.month
    df_date['calendar_quarter']=df_date['date'].dt.quarter
    df_date['calendar_year']=df_date['date'].dt.year
    df_date['calendar_semester']=(((df_date['date'].dt.month-1)//6)+1)
    df_date['fiscal_year']=df_date['date'].apply(lambda d: d.year if d.month <=7 else d.year+1)
    df_date['fiscal_month']=df_date['date'].apply(lambda d:((d.month)-7)%12+1)
    df_date['fiscal_quarter']=((df_date['fiscal_month']-1)//3) + 1
    df_date['fiscal_semester']=((df_date['fiscal_month']-1)//6) + 1

    df_date.drop('fiscal_month',inplace=True,axis=1)

    df_date['english_day_name_of_week'] = df_date['day_number_of_week'].map(english_days)
    df_date['spanish_day_name_of_week'] = df_date['day_number_of_week'].map(spanish_days)
    df_date['french_day_name_of_week'] = df_date['day_number_of_week'].map(french_days)

    df_date['english_month_name'] = df_date['month_number_of_year'].map(english_months)
    df_date['spanish_month_name'] = df_date['month_number_of_year'].map(spanish_months)
    df_date['french_month_name'] = df_date['month_number_of_year'].map(french_months)

    df_date['date_key'] = df_date['date'].dt.strftime('%Y%m%d').astype(int)

    df_date.rename(columns={'date': 'full_date_alternate_key'}, inplace=True)

    return df_date

def transform_promotion(df: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando promotion")
    df = df.rename(columns={
        'special_offer_id': 'promotion_alternate_key',
        'description': 'english_promotion_name',
        'type': 'english_promotion_type',
        'category': 'english_promotion_category',
    })

    df.drop(['rowguid', 'modified_date'], inplace=True, axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df=convert_language('english_promotion_name','french_promotion_name', tokenizer_fr, model_fr, df)
    df=convert_language('english_promotion_name','spanish_promotion_name', tokenizer_es, model_es, df)
    df=convert_language('english_promotion_category','french_promotion_category', tokenizer_fr, model_fr, df)
    df=convert_language('english_promotion_category','spanish_promotion_category',tokenizer_es, model_es, df)
    df=convert_language('english_promotion_type','french_promotion_type', tokenizer_fr, model_fr, df)
    df=convert_language('english_promotion_type','spanish_promotion_type',tokenizer_es, model_es, df)

    return df

def transform_product_category(df: pd.DataFrame, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando product category")
    df = df.rename(columns={'product_category_id':'product_category_alternate_key','name':'english_product_category_name'})
    df = df.drop(['rowguid', 'modified_date'], axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df = convert_language('english_product_category_name', 'spanish_product_category_name', tokenizer_es, model_es, df)
    df = convert_language('english_product_category_name', 'french_product_category_name', tokenizer_fr, model_fr, df)

    return df

def transform_product_subcategory(df: pd.DataFrame, etl_conn: Engine, model_registry: ModelRegistry) -> pd.DataFrame:
    print("TRANSFORM: Transformando product subcategory")
    df = df.rename(columns={'product_subcategory_id': 'product_subcategory_alternate_key', 'name': 'english_product_subcategory_name'})
    df=df.drop(['rowguid','modified_date'],axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df = convert_language('english_product_subcategory_name', 'spanish_product_subcategory_name', tokenizer_es, model_es, df)
    df = convert_language('english_product_subcategory_name', 'french_product_subcategory_name', tokenizer_fr, model_fr, df)

    dim_product_category = pd.read_sql_table('dim_product_category', etl_conn)

    df = df.merge(
        dim_product_category[['product_category_alternate_key', 'product_category_key']],
        left_on='product_category_id',
        right_on='product_category_alternate_key',
        how='left'
    )

    df.drop(['product_category_alternate_key', 'product_category_id'], inplace=True, axis=1)

    return df
