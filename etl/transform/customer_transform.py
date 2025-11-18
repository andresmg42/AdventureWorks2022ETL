import pandas as pd
from etl.transform.utils_transform import parse_xml_to_dict, get_upper_income
from utils.model_loader import ModelRegistry
from utils.translate_language import convert_language


def transforms_customer(df_customer_base: pd.DataFrame, df_dim_geography: pd.DataFrame, model_registry: ModelRegistry):
    print("TRANSFORM: Transformando customer")

    customer_demographics_mapping = {
        'birth_date': 'BirthDate', 'gender': 'Gender', 'marital_status': 'MaritalStatus',
        'yearly_income': 'YearlyIncome', 'total_children': 'TotalChildren',
        'number_children_at_home': 'NumberChildrenAtHome', 'education': 'Education',
        'occupation': 'Occupation', 'house_owner_flag': 'HomeOwnerFlag',
        'number_cars_owned': 'NumberCarsOwned', 'date_first_purchase': 'DateFirstPurchase',
        'commute_distance': 'CommuteDistance'
    }


    demographics_list = df_customer_base['demographics'].apply(
        lambda xml: parse_xml_to_dict(xml, customer_demographics_mapping)
    ).tolist()
    demographics_df = pd.DataFrame(demographics_list, index=df_customer_base.index)

    df_customer = pd.concat([df_customer_base.drop(columns=['demographics']), demographics_df], axis=1)

    df_customer = df_customer.rename(columns={'occupation':'english_occupation','education':'english_education'})

    tokenizer_es, model_es = model_registry.get_model('en', 'es')
    tokenizer_fr, model_fr = model_registry.get_model('en', 'fr')

    df_customer = convert_language('english_education', 'spanish_education', tokenizer_es, model_es, df_customer)
    df_customer = convert_language('english_education', 'french_education', tokenizer_fr, model_fr, df_customer)
    df_customer = convert_language('english_education', 'spanish_occupation', tokenizer_es, model_es, df_customer)
    df_customer = convert_language('english_occupation', 'french_occupation', tokenizer_fr, model_fr, df_customer)

    linking_columns = ['city', 'postal_code', 'state_province_code', 'country_region_code']
    df_customer = df_customer.merge(df_dim_geography, on=linking_columns, how='left')

    df_customer['yearly_income'] = get_upper_income(df_customer['yearly_income'])

    final_columns = [
        'customer_alternate_key', 'geography_key', 'title', 'first_name', 'middle_name',
        'last_name', 'suffix', 'name_style', 'birth_date', 'gender', 'marital_status',
        'yearly_income', 'total_children', 'number_children_at_home', 'english_education',
        'spanish_education', 'english_occupation', 'spanish_occupation', 'house_owner_flag',
        'number_cars_owned', 'email_address', 'phone', 'address_line1', 'address_line2',
        'date_first_purchase', 'commute_distance', 'french_occupation', 'french_education'
    ]

    df_customer = df_customer[final_columns]

    return df_customer