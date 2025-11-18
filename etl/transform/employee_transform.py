import pandas as pd
import numpy as np
from etl.transform.utils_transform import get_sales_employee_image


def transform_employee(
        df_employee_base: pd.DataFrame,
        df_emergency_contact: pd.DataFrame,
        df_sales_person: pd.DataFrame,
        df_pay_frequency: pd.DataFrame,
        df_dim_sales_territory: pd.DataFrame,
        df_base_rate: pd.DataFrame,
) -> pd.DataFrame:
    print("TRANSFORM: Transformando employee")

    df_employee = (
        df_employee_base
        .merge(df_sales_person, on='employee_alternate_key', how='left')
        .merge(
            df_dim_sales_territory,
            left_on='territory_id',
            right_on='sales_territory_alternate_key',
            how='left'
        )
        .drop(columns=['territory_id', 'sales_territory_alternate_key'])
        .merge(
            df_base_rate,
            left_on='employee_national_id_alternate_key',
            right_on='national_idnumber',
            how='left'
        )
        .merge(
            df_emergency_contact,
            left_on='employee_national_id_alternate_key',
            right_on='national_idnumber',
            how='left'
        )
        .merge(
            df_pay_frequency,
            left_on='employee_national_id_alternate_key',
            right_on='national_idnumber',
            how='left'
        )
    )

    df_employee['name_style'] = 0
    df_employee['sales_person_flag'] = df_employee['sales_territory_key'].notna().astype(int)
    df_employee['current_flag'] = df_employee['current_flag'].astype(int)

    df_employee['status'] = np.where(df_employee['end_date'].isna(), 'Current', None)

    df_employee['salaried_flag'] = df_employee['salaried_flag'].astype(int)

    fill_values = {'department_name': 'Unknown', 'title': 'Unknown', 'job_title': 'Unknown'}
    df_employee = df_employee.fillna(value=fill_values)

    df_employee['employee_photo'] = df_employee.apply(get_sales_employee_image, axis=1)

    cols_to_drop = [
        'national_idnumber_x', 'suffix', 'organization_level',
        'national_idnumber_y', 'employee_alternate_key', 'job_title', 'national_idnumber'
    ]
    df_employee = df_employee.drop(columns=cols_to_drop, errors='ignore')

    return df_employee