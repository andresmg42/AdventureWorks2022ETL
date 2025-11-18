import pandas as pd
import numpy as np

def transforms_fact_internet_sales(
        df_internet_sales_base: pd.DataFrame,
        df_special_offer: pd.DataFrame,
        df_dim_product: pd.DataFrame,
        df_dim_promotion: pd.DataFrame,
        df_dim_currency: pd.DataFrame,
        df_dim_sales_territory: pd.DataFrame,
        df_dim_customer: pd.DataFrame
) -> pd.DataFrame:
    print("TRANSFORM: Transformando fact internet sales")

    product_dim_clean = df_dim_product.drop_duplicates(subset=['product_alternate_key'])
    special_offer_clean = df_special_offer.drop_duplicates(subset=['product_number'])

    df_fact_sales = (
        df_internet_sales_base
        .merge(
            product_dim_clean[['product_key', 'product_alternate_key']],
            left_on='product_number',
            right_on='product_alternate_key',
            how='left'
        )
        .merge(
            special_offer_clean,
            on='product_number',
            how='left'
        )
        .merge(
            df_dim_promotion[['promotion_alternate_key', 'promotion_key']],
            left_on='special_offer_id',
            right_on='promotion_alternate_key',
            how='left'
        )
        .merge(
            df_dim_currency[['currency_key', 'currency_alternate_key']],
            left_on='to_currency_code',
            right_on='currency_alternate_key',
            how='left'
        )
        .merge(
            df_dim_sales_territory[['sales_territory_key', 'sales_territory_alternate_key']],
            left_on='territory_id',
            right_on='sales_territory_alternate_key',
            how='left'
        )
        .merge(
            df_dim_customer[['customer_key', 'customer_alternate_key']],
            left_on='account_number',
            right_on='customer_alternate_key',
            how='left'
        )
    )


    df_fact_sales = df_fact_sales.assign(
        revision_number=np.where(df_fact_sales['revision_number'] == 8, 1, 2),
        order_date_key=df_fact_sales['order_date'].dt.strftime('%Y%m%d').astype(int),
        due_date_key=df_fact_sales['due_date'].dt.strftime('%Y%m%d').astype(int),
        ship_date_key=df_fact_sales['ship_date'].dt.strftime('%Y%m%d').astype(int)
    )

    cols_to_drop = [
        'product_number', 'product_alternate_key', 'special_offer_id',
        'promotion_alternate_key', 'to_currency_code', 'currency_alternate_key',
        'territory_id', 'sales_territory_alternate_key', 'account_number',
        'customer_alternate_key'
    ]

    df_fact_sales = df_fact_sales.drop(columns=cols_to_drop, errors='ignore')

    return df_fact_sales
