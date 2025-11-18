import pandas as pd


def _filter_by_product_validity_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función auxiliar. Filtra las ventas para quedarse
    solo con los registros de producto que eran válidos en la fecha de la orden.
    """
    historical_condition = (df['order_date'] >= df['start_date']) & (df['order_date'] <= df['end_date'])

    actual_condition = (df['order_date'] >= df['start_date']) & (df['end_date'].isna())

    return df[historical_condition | actual_condition].copy()

def transforms_fact_reseller_sales(
        df_fact_reseller_sales: pd.DataFrame,
        df_special_offer: pd.DataFrame,
        df_table_currency: pd.DataFrame,
        df_dim_product: pd.DataFrame,
        df_dim_promotion: pd.DataFrame,
        df_dim_currency: pd.DataFrame,
        df_dim_sales_territory: pd.DataFrame,
        df_dim_employee: pd.DataFrame,
        df_dim_reseller: pd.DataFrame,
) -> pd.DataFrame:
    print("TRANSFORM: Transformando fact reseller sales (Optimizado)")

    product_dim_clean = df_dim_product.copy()
    product_dim_clean['start_date'] = pd.to_datetime(product_dim_clean['start_date']).dt.tz_localize('UTC')
    product_dim_clean['end_date'] = pd.to_datetime(product_dim_clean['end_date']).dt.tz_localize('UTC')

    df_fact_sales = (
        df_fact_reseller_sales
        .merge(
            product_dim_clean,
            left_on='product_number',
            right_on='product_alternate_key',
            how='left'
        )
        .pipe(_filter_by_product_validity_date)
        .merge(
            df_dim_reseller[['reseller_key', 'reseller_alternate_key']],
            left_on='reseller_account_number',
            right_on='reseller_alternate_key',
            how='left'
        )
        .merge(
            df_dim_employee[['employee_key', 'employee_national_id_alternate_key']],
            left_on='employee_national_id',
            right_on='employee_national_id_alternate_key',
            how='left'
        )
        .merge(
            df_special_offer,
            left_on='sales_order_detail_id',
            right_index=True,
            how='left'
        )
        .merge(
            df_dim_promotion[['promotion_key', 'promotion_alternate_key']],
            left_on='special_offer_id',
            right_on='promotion_alternate_key',
            how='left'
        )
        .merge(
            df_table_currency,
            on='currency_rate_id',
            how='left'
        ).drop(columns=['currency_rate_id'])
        .assign(
            to_currency_code=lambda df: df['to_currency_code'].fillna('USD')
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
    )

    df_fact_sales = df_fact_sales.assign(
        order_date_key=df_fact_sales['order_date'].dt.strftime('%Y%m%d').astype(int),
        due_date_key=df_fact_sales['due_date'].dt.strftime('%Y%m%d').astype(int),
        ship_date_key=df_fact_sales['ship_date'].dt.strftime('%Y%m%d').astype(int)
    )

    final_columns = [
        'product_key', 'order_date_key', 'due_date_key', 'ship_date_key', 'reseller_key',
        'employee_key', 'promotion_key', 'currency_key', 'sales_territory_key',
        'sales_order_number', 'sales_order_line_number', 'revision_number', 'order_quantity',
        'unit_price', 'extended_amount', 'unit_price_discount_pct', 'discount_amount',
        'product_standard_cost', 'total_product_cost', 'sales_amount', 'tax_amt',
        'freight', 'carrier_tracking_number', 'customer_po_number', 'order_date',
        'due_date', 'ship_date'
    ]

    existing_final_columns = [col for col in final_columns if col in df_fact_sales.columns]

    return df_fact_sales[existing_final_columns]