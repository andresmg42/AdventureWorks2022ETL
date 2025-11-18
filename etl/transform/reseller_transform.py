import pandas as pd
from etl.transform.utils_transform import parse_xml_to_dict


def transform_reseller(df_reseller_base: pd.DataFrame, df_dim_geography: pd.DataFrame):
    print("TRANSFORM: Transformando reseller")

    reseller_demographics_mapping = {
        'annual_sales': 'AnnualSales',
        'annual_revenue': 'AnnualRevenue',
        'bank_name': 'BankName',
        'business_type': 'BusinessType',
        'year_opened': 'YearOpened',
        'product_line': 'Specialty',
        'number_employees': 'NumberEmployees'
    }

    demographics_list = df_reseller_base['demographics'].apply(
        lambda xml: parse_xml_to_dict(xml, reseller_demographics_mapping)
    ).tolist()
    demographics_df = pd.DataFrame(demographics_list, index=df_reseller_base.index)

    demographics_df['min_payment_type'] = None
    demographics_df['min_payment_amount'] = None

    df_reseller = pd.concat([df_reseller_base.drop(columns=['demographics']), demographics_df], axis=1)

    business_type_map = {
        'BM': 'Warehouse',
        'OS': 'Value Added Reseller',
        'BS': 'Specialty Bike Shop'
    }
    df_reseller['business_type'] = df_reseller['business_type'].map(business_type_map)

    df_reseller = df_reseller.merge(
        df_dim_geography,
        on=['city', 'postal_code', 'state_province_name', 'country_region_name'],
        how='left'
    )

    fill_values = {'business_type': 'Unknown', 'product_line': 'None'}
    df_reseller = df_reseller.fillna(value=fill_values)

    final_columns = [
        'geography_key', 'reseller_alternate_key', 'phone', 'business_type',
        'reseller_name', 'number_employees', 'order_frequency', 'order_month',
        'first_order_year', 'last_order_year', 'product_line', 'address_line1',
        'address_line2', 'annual_sales', 'bank_name', 'min_payment_type',
        'min_payment_amount', 'annual_revenue', 'year_opened'
    ]

    existing_final_columns = [col for col in final_columns if col in df_reseller.columns]

    return df_reseller[existing_final_columns]