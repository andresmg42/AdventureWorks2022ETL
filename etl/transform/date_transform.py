import pandas as pd
import numpy as np
from utils.days_and_moths import english_days, english_months, spanish_days, spanish_months, french_days, french_months

def transform_date() -> pd.DataFrame:
    print("TRANSFORM: Generando date")
    df_date = pd.DataFrame({
        "date": pd.date_range(start='1/1/2005', end='31/12/2014', freq='D')
    })

    date_series = df_date['date']
    df_date['day_number_of_week'] = ((date_series.dt.day_of_week + 1) % 7) + 1 # Domingo=1, Lunes=2
    df_date['day_number_of_month'] = date_series.dt.day
    df_date['day_number_of_year'] = date_series.dt.dayofyear
    df_date['month_number_of_year'] = date_series.dt.month
    df_date['calendar_quarter'] = date_series.dt.quarter
    df_date['calendar_year'] = date_series.dt.year
    df_date['calendar_semester'] = ((date_series.dt.month - 1) // 6) + 1

    start_of_year = pd.to_datetime(date_series.dt.year, format='%Y')
    days_from_start = (date_series - start_of_year).dt.days
    start_of_year_weekday = start_of_year.dt.weekday
    df_date['week_number_of_year'] = (days_from_start + start_of_year_weekday + 1) // 7 + 1

    df_date['fiscal_year'] = np.where(
        date_series.dt.month <= 7,
        date_series.dt.year,
        date_series.dt.year + 1
    )

    fiscal_month = ((date_series.dt.month - 7 - 1) % 12) + 1
    df_date['fiscal_quarter'] = ((fiscal_month - 1) // 3) + 1
    df_date['fiscal_semester'] = ((fiscal_month - 1) // 6) + 1

    df_date['english_day_name_of_week'] = df_date['day_number_of_week'].map(english_days)
    df_date['spanish_day_name_of_week'] = df_date['day_number_of_week'].map(spanish_days)
    df_date['french_day_name_of_week'] = df_date['day_number_of_week'].map(french_days)

    df_date['english_month_name'] = df_date['month_number_of_year'].map(english_months)
    df_date['spanish_month_name'] = df_date['month_number_of_year'].map(spanish_months)
    df_date['french_month_name'] = df_date['month_number_of_year'].map(french_months)

    df_date['date_key'] = df_date['date'].dt.strftime('%Y%m%d').astype(int)

    df_date = df_date.rename(columns={'date': 'full_date_alternate_key'})

    return df_date