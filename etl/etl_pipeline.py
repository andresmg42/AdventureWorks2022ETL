from etl.extract.etl_context import ETLContext


def sales_territory_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_sales_territory
    from etl.transform.sales_territory_transform import transform_sales_territory
    from etl.load import load_to_dw

    df_sales_territory_raw = extract_sales_territory(ctx.oltp_engine, 'sales')
    df_sales_territory_final = transform_sales_territory(df_sales_territory_raw)
    load_to_dw(df_sales_territory_final, 'dim_sales_territory', ctx.dest_schema, ctx.dw_engine)

def currency_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_currency
    from etl.transform.currency_transform import transform_currency
    from etl.load import load_to_dw

    df_currency_raw = extract_currency(ctx.oltp_engine)
    df_currency_final = transform_currency(df_currency_raw)
    load_to_dw(df_currency_final, 'dim_currency', ctx.dest_schema, ctx.dw_engine)

def geography_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_geography
    from etl.transform.geography_transform import transform_geography
    from etl.load import load_to_dw

    df_geography_raw = extract_geography(ctx.oltp_engine)
    df_geography_final = transform_geography(df_geography_raw, ctx.dim_cache.get("dim_sales_territory"), ctx.model_registry)
    load_to_dw(df_geography_final, 'dim_geography', ctx.dest_schema, ctx.dw_engine)

def date_pipeline(ctx: ETLContext):
    from etl.transform.date_transform import transform_date
    from etl.load import load_to_dw
    df_date_final = transform_date()
    load_to_dw(df_date_final, 'dim_date', ctx.dest_schema, ctx.dw_engine)

def promotion_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_promotion
    from etl.transform.promotion_transform import transform_promotion
    from etl.load import load_to_dw

    df_promotion_raw = extract_promotion(ctx.oltp_engine, 'sales')
    df_promotion_final = transform_promotion(df_promotion_raw, ctx.model_registry)
    load_to_dw(df_promotion_final, 'dim_promotion', ctx.dest_schema, ctx.dw_engine)

def product_category_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_product_category
    from etl.transform.product_transform.product_category_transform import transform_product_category
    from etl.load import load_to_dw

    df_product_category_raw = extract_product_category(ctx.oltp_engine, 'production')
    df_product_category_final = transform_product_category(df_product_category_raw, ctx.model_registry)
    load_to_dw(df_product_category_final, 'dim_product_category', ctx.dest_schema, ctx.dw_engine)


def product_subcategory_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_product_subcategory
    from etl.transform.product_transform.product_subcategory_transform import transform_product_subcategory
    from etl.load import load_to_dw

    df_product_subcategory_raw = extract_product_subcategory(ctx.oltp_engine, 'production')
    df_product_subcategory_final = transform_product_subcategory(df_product_subcategory_raw, ctx.dim_cache.get("dim_product_category"), ctx.model_registry)
    load_to_dw(df_product_subcategory_final, 'dim_product_subcategory', ctx.dest_schema, ctx.dw_engine)

def product_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_product, extract_sales_order, extract_product_model, extract_large_photo, extract_product_price_list, extract_language_description
    from etl.transform.product_transform.product_transform import transform_product
    from etl.transform.product_transform.language_description_transform import transform_language_description
    from etl.load import load_to_dw

    co_oltp = ctx.oltp_engine

    df_product_raw = extract_product(co_oltp, 'production')
    df_sales_order = extract_sales_order(co_oltp)
    df_product_model = extract_product_model(co_oltp)
    df_large_photo = extract_large_photo(co_oltp)
    df_product_price_list = extract_product_price_list(co_oltp)
    df_language_description_raw = extract_language_description(co_oltp)
    df_pivoted_language_description = transform_language_description(df_language_description_raw)
    df_product_raw_final = transform_product(
        df_product_raw,
        df_sales_order,
        df_product_model,
        df_large_photo,
        df_product_price_list,
        df_pivoted_language_description,
        ctx.dim_cache.get("dim_product_subcategory"),
        ctx.model_registry
    )
    load_to_dw(df_product_raw_final, 'dim_product', ctx.dest_schema, ctx.dw_engine)

def customer_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_customer
    from etl.transform.customer_transform import transforms_customer
    from etl.load import load_to_dw

    df_customer_raw = extract_customer(ctx.oltp_engine)
    df_customer_final = transforms_customer(df_customer_raw, ctx.dim_cache.get("dim_geography"), ctx.model_registry)
    load_to_dw(df_customer_final, 'dim_customer', ctx.dest_schema, ctx.dw_engine)

def employee_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_employee, extract_emergency_contact_data, extract_sales_person, extract_pay_frequency, extract_base_rate
    from etl.transform.employee_transform import transform_employee
    from etl.load import load_to_dw

    co_oltp = ctx.oltp_engine

    df_employee_raw = extract_employee(co_oltp)
    df_emergency_contact = extract_emergency_contact_data(co_oltp)
    df_sales_person = extract_sales_person(co_oltp)
    df_pay_frequency = extract_pay_frequency(co_oltp)
    df_base_rate = extract_base_rate(co_oltp)
    df_employee_final = transform_employee(
        df_employee_raw,
        df_emergency_contact,
        df_sales_person,
        df_pay_frequency,
        ctx.dim_cache.get("dim_sales_territory"),
        df_base_rate,
    )
    load_to_dw(df_employee_final, 'dim_employee', ctx.dest_schema, ctx.dw_engine)


def reseller_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_reseller
    from etl.transform.reseller_transform import transform_reseller
    from etl.load import load_to_dw

    df_reseller_raw = extract_reseller(ctx.oltp_engine)
    df_reseller_final = transform_reseller(df_reseller_raw, ctx.dim_cache.get("dim_geography"))
    load_to_dw(df_reseller_final, 'dim_reseller', ctx.dest_schema, ctx.dw_engine)

def fact_internet_sales_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_fact_internet_sales, extract_special_offer_internet_sales
    from etl.transform.fact_internet_sales_transform import transforms_fact_internet_sales
    from etl.load import load_to_dw

    df_fact_internet_sales_raw = extract_fact_internet_sales(ctx.oltp_engine)
    df_special_offer = extract_special_offer_internet_sales(ctx.oltp_engine)
    df_fact_internet_sales_final = transforms_fact_internet_sales(
        df_fact_internet_sales_raw,
        df_special_offer,
        ctx.dim_cache.get("dim_product"),
        ctx.dim_cache.get("dim_promotion"),
        ctx.dim_cache.get("dim_currency"),
        ctx.dim_cache.get("dim_sales_territory"),
        ctx.dim_cache.get("dim_customer"),
    )
    load_to_dw(df_fact_internet_sales_final, 'fact_internet_sales', ctx.dest_schema, ctx.dw_engine)

def fact_reseller_sales_pipeline(ctx: ETLContext):
    from etl.extract.extract import extract_fact_reseller_sales, extract_special_offer_reseller_sales, extract_currency_reseller_sales
    from etl.transform.fact_reseller_sales_transform import transforms_fact_reseller_sales
    from etl.load import load_to_dw

    df_fact_reseller_sales_raw = extract_fact_reseller_sales(ctx.oltp_engine)
    df_special_offer_reseller_sales = extract_special_offer_reseller_sales(ctx.oltp_engine)
    df_currency_reseller_sales = extract_currency_reseller_sales(ctx.oltp_engine)
    df_fact_reseller_sales_final = transforms_fact_reseller_sales(
        df_fact_reseller_sales_raw,
        df_special_offer_reseller_sales,
        df_currency_reseller_sales,
        ctx.dim_cache.get("dim_product"),
        ctx.dim_cache.get("dim_promotion"),
        ctx.dim_cache.get("dim_currency"),
        ctx.dim_cache.get("dim_sales_territory"),
        ctx.dim_cache.get("dim_employee"),
        ctx.dim_cache.get("dim_reseller"),
    )
    load_to_dw(df_fact_reseller_sales_final, 'fact_reseller_sales', ctx.dest_schema, ctx.dw_engine)