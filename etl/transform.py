import pandas as pd
from sqlalchemy import text, Engine
from etl.utils_etl import get_sales_territory_image, get_sales_employee_image, get_size_range, extract_demographics, parse_store_demographics, upper_income
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

def transform_product_subcategory(df: pd.DataFrame, etl_conn: Engine, model_registry: ModelRegistry,SCHEMA) -> pd.DataFrame:
    print("TRANSFORM: Transformando product subcategory")
    df = df.rename(columns={'product_subcategory_id': 'product_subcategory_alternate_key', 'name': 'english_product_subcategory_name'})
    df=df.drop(['rowguid','modified_date'],axis=1)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df = convert_language('english_product_subcategory_name', 'spanish_product_subcategory_name', tokenizer_es, model_es, df)
    df = convert_language('english_product_subcategory_name', 'french_product_subcategory_name', tokenizer_fr, model_fr, df)

    dim_product_category = pd.read_sql_table('dim_product_category', etl_conn, schema=SCHEMA)

    df = df.merge(
        dim_product_category[['product_category_alternate_key', 'product_category_key']],
        left_on='product_category_id',
        right_on='product_category_alternate_key',
        how='left'
    )

    df.drop(['product_category_alternate_key', 'product_category_id'], inplace=True, axis=1)

    return df

def transform_language_description(df_language_description: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM: Transformando language description")
    columns = df_language_description['name'].unique().tolist()

    new_columns = {name: f'{name.lower()}_description' for name in columns}
    df_language_description = df_language_description.pivot(
        index='product_id', columns='name',
        values='description').reset_index()

    df_language_description.rename(columns=new_columns, inplace=True)

    return df_language_description


def transform_product(
        df: pd.DataFrame,
        df_sales_order: pd.DataFrame,
        df_product_model: pd.DataFrame,
        df_large_photo: pd.DataFrame,
        df_product_price_list: pd.DataFrame,
        df_pivoted_descriptions: pd.DataFrame,
        etl_conn: Engine,
        model_registry: ModelRegistry
) -> pd.DataFrame:
    print("TRANSFORM: Transformando product")
    df = df.rename(columns={'product_number': 'product_alternate_key', 'name': 'english_product_name'})

    dim_product_subcategory = pd.read_sql_query(
        """SELECT product_subcategory_key, product_subcategory_alternate_key
        FROM dw.dim_product_subcategory
        """,
        etl_conn
    )

    df = df.merge(
        dim_product_subcategory[['product_subcategory_alternate_key','product_subcategory_key']],
        left_on='product_subcategory_id',
        right_on='product_subcategory_alternate_key',
        how='left'
    )

    df.drop(['product_subcategory_alternate_key','product_subcategory_id'], axis=1 ,inplace=True)
    df['size_range'] = df['size'].apply(get_size_range)

    df = df.merge(
        df_sales_order[['product_id', 'dealer_price']],
        on='product_id',
        how='left'
    )

    df = df.merge(
        df_product_model,
        on='product_model_id',
        how='left'
    ).rename(columns={'name': 'model_name'})

    df = df.merge(
        df_large_photo,
        on='product_id',
        how='left'
    )

    df = df.merge(
        df_product_price_list,
        on='product_id',
        how='left'
    )

    df['status'] = df['end_date'].apply(lambda x: 'Current' if pd.isna(x) else None)

    df = df.merge(
        df_pivoted_descriptions,
        on='product_id',
        how='left'
    )

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')
    tokenizer_jap, model_jap = model_registry.get_model('en', 'jap')
    tokenizer_de, model_de = model_registry.get_model('en', 'de')
    tokenizer_trk, model_trk = model_registry.get_model('en', 'trk')

    df=convert_language('english_product_name','french_product_name', tokenizer_fr, model_fr, df)
    df=convert_language('english_product_name','spanish_product_name', tokenizer_es, model_es, df)

    df=convert_language('english_description','japanese_description', tokenizer_jap, model_jap, df)
    df=convert_language('english_description','german_description',tokenizer_de, model_de, df)
    df=convert_language('english_description','turkish_description', tokenizer_trk, model_trk, df)

    df=df.drop(['product_id','make_flag','product_model_id','discontinued_date','rowguid','modified_date','sell_start_date','sell_end_date'],axis=1)

    df['color'] = df['color'].apply(lambda x: 'NA' if pd.isna(x) else x)
    df = df.replace({'nan': None})

    return df

def transforms_customer(df_customer_base: pd.DataFrame, etl_conn: Engine, model_registry: ModelRegistry):
    print("TRANSFORM: Transformando customer")
    demographics_df = df_customer_base['demographics'].apply(extract_demographics).apply(pd.Series)
    df_customer_base = pd.concat([df_customer_base.drop('demographics', axis=1), demographics_df], axis=1)

    df_customer_base.rename(columns={'occupation':'english_occupation','education':'english_education'},inplace=True)

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_customer_base = convert_language('english_education', 'spanish_education', tokenizer_es, model_es, df_customer_base)
    df_customer_base = convert_language('english_education', 'french_education', tokenizer_fr, model_fr, df_customer_base)
    df_customer_base = convert_language('english_education', 'spanish_occupation', tokenizer_es, model_es, df_customer_base)
    df_customer_base = convert_language('english_occupation', 'french_occupation', tokenizer_fr, model_fr, df_customer_base)

    df_geo_with_keys = pd.read_sql(
        """
             SELECT geography_key, city, postal_code, state_province_code, country_region_code
             FROM dw.dim_geography;
        """,
        etl_conn)

    linking_columns = ['city', 'postal_code', 'state_province_code', 'country_region_code']

    df_customer_base = pd.merge(
        df_customer_base,
        df_geo_with_keys,
        on=linking_columns,
        how='left'
    )

    final_columns = [
        'customer_alternate_key', 'geography_key', 'title', 'first_name', 'middle_name',
        'last_name', 'suffix', 'name_style', 'birth_date', 'gender', 'marital_status',
        'yearly_income', 'total_children', 'number_children_at_home', 'english_education',
        'spanish_education', 'english_occupation', 'spanish_occupation', 'house_owner_flag',
        'number_cars_owned', 'email_address', 'phone', 'address_line1', 'address_line2',
        'date_first_purchase', 'commute_distance', 'french_occupation', 'french_education'
    ]

    df_customer_base = df_customer_base[final_columns]
    df_customer_base['yearly_income'] = df_customer_base['yearly_income'].apply(upper_income)

    return df_customer_base

def transform_employee(
        df_employee_base: pd.DataFrame,
        df_emergency_contact: pd.DataFrame,
        df_sales_person: pd.DataFrame,
        df_pay_frequency: pd.DataFrame,
        df_base_rate: pd.DataFrame,
        etl_conn: Engine,
) -> pd.DataFrame:
    print("TRANSFORM: Transformando employee")

    df_sales_territory = pd.read_sql(
        text("SELECT sales_territory_key, sales_territory_alternate_key FROM dw.dim_sales_territory;"),
        etl_conn
    )

    df_employee_base = df_employee_base.merge(
        df_sales_person,
        on='employee_alternate_key',
        how='left'
    )

    df_employee_base = df_employee_base.merge(
        df_sales_territory,
        left_on='territory_id',
        right_on='sales_territory_alternate_key',
        how='left'
    ).drop(['territory_id', 'sales_territory_alternate_key'], axis=1)

    df_employee_base['name_style'] = 0
    df_employee_base['sales_person_flag'] = df_employee_base['sales_territory_key'].notnull().astype(int)
    df_employee_base['current_flag'] = df_employee_base['current_flag'].astype(int)

    for col in ['department_name', 'title', 'job_title']:
        df_employee_base[col] = df_employee_base[col].fillna('Unknown')

    df_employee_base = df_employee_base.merge(
        df_base_rate,
        left_on='employee_national_id_alternate_key',
        right_on='national_idnumber',
        how='left'
    )

    df_employee_base = df_employee_base.merge(
        df_emergency_contact,
        left_on='employee_national_id_alternate_key',
        right_on='national_idnumber',
        how='left'
    )

    df_employee_base['employee_photo'] = df_employee_base.apply(get_sales_employee_image,axis=1)

    df_employee_base = df_employee_base.merge(
        df_pay_frequency,
        right_on='national_idnumber',
        left_on='employee_national_id_alternate_key',
        how='left'
    )

    df_employee_base['status'] = df_employee_base['end_date'].apply(lambda x: 'Current' if x is None else None)
    df_employee_base['salaried_flag'] = df_employee_base['salaried_flag'].apply(lambda x: 1 if x else 0)

    df_employee_base=df_employee_base.drop(
        ['national_idnumber_x', 'suffix', 'organization_level',
         'national_idnumber_y', 'employee_alternate_key', 'job_title', 'national_idnumber'],
        axis=1
    )

    return df_employee_base

def transform_reseller(df_reseller_base: pd.DataFrame, etl_conn: Engine):
    print("TRANSFORM: Transformando reseller")
    demographics_df = df_reseller_base['demographics'].apply(parse_store_demographics).apply(pd.Series)
    df_reseller_base = pd.concat([df_reseller_base.drop('demographics', axis=1), demographics_df], axis=1)

    business_type_map = {
        'BM': 'Warehouse',
        'OS': 'Value Added Reseller',
        'BS': 'Specialty Bike Shop'
    }

    df_reseller_base['business_type'] = df_reseller_base['business_type'].map(business_type_map)

    df_geo_with_keys = pd.read_sql(
        """
            SELECT geography_key, city, postal_code, state_province_name, english_country_region_name as country_region_name
            FROM dw.dim_geography;
        """,
        etl_conn)

    df_reseller_base = df_reseller_base.merge(
        df_geo_with_keys,
        on=['city', 'postal_code', 'state_province_name', 'country_region_name'],
        how='left'
    )

    df_reseller_base['business_type'] = df_reseller_base['business_type'].fillna('Unknown')
    df_reseller_base['product_line'] = df_reseller_base['product_line'].fillna('None')

    final_columns = [
        'geography_key', 'reseller_alternate_key', 'phone', 'business_type',
        'reseller_name', 'number_employees', 'order_frequency', 'order_month',
        'first_order_year', 'last_order_year', 'product_line', 'address_line1',
        'address_line2', 'annual_sales', 'bank_name', 'min_payment_type',
        'min_payment_amount', 'annual_revenue', 'year_opened'
    ]

    df_reseller_base = df_reseller_base[final_columns]

    return df_reseller_base


def transforms_fact_internet_sales(df: pd.DataFrame, df_special_offer: pd.DataFrame, etl_conn: Engine,SCHEMA) -> pd.DataFrame:
    print("TRANSFORM: Transformando fact internet sales")
    df_product = pd.read_sql_table('dim_product', etl_conn,SCHEMA)
    df_promotion = pd.read_sql_table('dim_promotion', etl_conn,SCHEMA)
    df_currency = pd.read_sql_table('dim_currency', etl_conn,SCHEMA)
    df_sales_territory = pd.read_sql_table('dim_sales_territory', etl_conn,SCHEMA)
    df_customer = pd.read_sql_table('dim_customer', etl_conn,SCHEMA)

    df_product = df_product.drop_duplicates(subset=['product_alternate_key'])
    df_special_offer.drop_duplicates(subset=['product_number'], inplace=True)

    df = df.merge(
        df_product[['product_key', 'product_alternate_key']],
        left_on='product_number',
        right_on='product_alternate_key',
        how='left'
    )

    df = df.merge(
        df_special_offer,
        on='product_number',
        how='left'
    ).drop(['product_number'], axis=1)

    df = df.merge(
        df_promotion[['promotion_alternate_key', 'promotion_key']],
        left_on='special_offer_id',
        right_on='promotion_alternate_key',
        how='left'
    ).drop(['special_offer_id', 'promotion_alternate_key'], axis=1)

    df = df.merge(
        df_currency[['currency_key', 'currency_alternate_key']],
        left_on='to_currency_code',
        right_on='currency_alternate_key',
        how='left'
    ).drop(['currency_alternate_key', 'to_currency_code'], axis=1)

    df = df.merge(
        df_sales_territory[['sales_territory_key', 'sales_territory_alternate_key']],
        left_on='territory_id',
        right_on='sales_territory_alternate_key',
        how='left'
    ).drop(['territory_id', 'sales_territory_alternate_key'], axis=1)

    df['revision_number'] = df['revision_number'].apply(lambda x: 1 if x == 8 else 2)

    df.drop(['product_alternate_key'], axis=1, inplace=True)

    df['order_date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
    df['due_date_key'] = df['due_date'].dt.strftime('%Y%m%d').astype(int)
    df['ship_date_key'] = df['ship_date'].dt.strftime('%Y%m%d').astype(int)

    df = df.merge(
        df_customer[['customer_key', 'customer_alternate_key']],
        left_on='account_number',
        right_on='customer_alternate_key',
        how='left'
    )

    df = df.drop(columns=['customer_alternate_key', 'account_number'])

    return df

def transforms_fact_reseller_tables(
        df_reseller_base: pd.DataFrame,
        df_special_offer: pd.DataFrame,
        df_table_currency: pd.DataFrame,
        etl_conn: Engine,
        SCHEMA
) -> pd.DataFrame:
    print("TRANSFORM: Transformando fact reseller sales")
    df_product = pd.read_sql_table('dim_product', etl_conn,SCHEMA)
    df_reseller = pd.read_sql_table('dim_reseller', etl_conn,SCHEMA)
    df_employee = pd.read_sql_table('dim_employee', etl_conn,SCHEMA)
    df_currency = pd.read_sql_table('dim_currency', etl_conn,SCHEMA)
    df_sales_territory = pd.read_sql_table('dim_sales_territory', etl_conn,SCHEMA)
    df_promotion = pd.read_sql_table('dim_promotion', etl_conn,SCHEMA)

    df_product['start_date'] = pd.to_datetime(df_product['start_date'])
    df_product['end_date'] = pd.to_datetime(df_product['end_date'])
    df_product['start_date'] = df_product['start_date'].dt.tz_localize('UTC')
    df_product['end_date'] = df_product['end_date'].dt.tz_localize('UTC')

    df_reseller_base = df_reseller_base.merge(
        df_product,
        left_on='product_number',
        right_on='product_alternate_key',
        how='left'
    )

    historical_condition = (df_reseller_base['order_date'] >= df_reseller_base['start_date']) & (
                df_reseller_base['order_date'] <= df_reseller_base['end_date'])

    actual_condition = ((df_reseller_base['order_date'] >= df_reseller_base['start_date'])
                       & (df_reseller_base['end_date'].isnull()))

    df_reseller_base = df_reseller_base[historical_condition | actual_condition].copy()

    df_reseller_base['reseller_account_number'] = df_reseller_base['reseller_account_number'].astype(str)
    df_reseller['reseller_alternate_key'] = df_reseller['reseller_alternate_key'].astype(str)

    df_reseller_base = df_reseller_base.merge(
        df_reseller[['reseller_key', 'reseller_alternate_key']],
        left_on='reseller_account_number',
        right_on='reseller_alternate_key',
        how='left'
    ).drop(['reseller_id', 'reseller_account_number', 'reseller_alternate_key'], axis=1)

    df_reseller_base['employee_national_id'] = df_reseller_base['employee_national_id'].astype(str)
    df_employee['employee_national_id_alternate_key'] = df_employee['employee_national_id_alternate_key'].astype(str)

    df_reseller_base = df_reseller_base.merge(
        df_employee[['employee_key', 'employee_national_id_alternate_key']],
        left_on='employee_national_id',
        right_on='employee_national_id_alternate_key',
        how='left'
    ).drop(['sales_person_id', 'employee_national_id', 'employee_national_id_alternate_key'], axis=1)

    df_reseller_base = df_reseller_base.merge(
        df_special_offer,
        left_on='sales_order_detail_id',
        right_index=True,
        how='left'
    )

    df_reseller_base = df_reseller_base.merge(
        df_promotion[['promotion_key', 'promotion_alternate_key']],
        left_on='special_offer_id',
        right_on='promotion_alternate_key',
        how='left'
    ).drop(['special_offer_id', 'promotion_alternate_key'], axis=1)

    df_reseller_base = df_reseller_base.merge(
        df_table_currency,
        on='currency_rate_id',
        how='left'
    )

    default_currency_code = 'USD'

    df_reseller_base['to_currency_code'] = df_reseller_base['to_currency_code'].fillna(default_currency_code)

    df_reseller_base.drop('currency_rate_id', axis=1, inplace=True)

    df_reseller_base = df_reseller_base.merge(
        df_currency[['currency_key', 'currency_alternate_key']],
        left_on='to_currency_code',
        right_on='currency_alternate_key',
        how='left'
    ).drop(['to_currency_code', 'currency_alternate_key'], axis=1)

    df_reseller_base = df_reseller_base.merge(
        df_sales_territory[['sales_territory_key', 'sales_territory_alternate_key']],
        left_on='territory_id',
        right_on='sales_territory_alternate_key',
        how='left'
    ).drop(['territory_id', 'sales_territory_alternate_key'], axis=1)

    df_reseller_base['order_date_key'] = df_reseller_base['order_date'].dt.strftime('%Y%m%d').astype(int)
    df_reseller_base['due_date_key'] = df_reseller_base['due_date'].dt.strftime('%Y%m%d').astype(int)
    df_reseller_base['ship_date_key'] = df_reseller_base['ship_date'].dt.strftime('%Y%m%d').astype(int)

    final_columns = [
        'product_key',
        'order_date_key',
        'due_date_key',
        'ship_date_key',
        'reseller_key',
        'employee_key',
        'promotion_key',
        'currency_key',
        'sales_territory_key',
        'sales_order_number',
        'sales_order_line_number',
        'revision_number',
        'order_quantity',
        'unit_price',
        'extended_amount',
        'unit_price_discount_pct',
        'discount_amount',
        'product_standard_cost',
        'total_product_cost',
        'sales_amount',
        'tax_amt',
        'freight',
        'carrier_tracking_number',
        'customer_po_number',
        'order_date',
        'due_date',
        'ship_date'
    ]

    df_reseller_base = df_reseller_base[final_columns]

    return df_reseller_base
