import pandas as pd
from sqlalchemy import text, Engine

def _read_table(co_oltp: Engine, schema: str, table_name: str) -> pd.DataFrame:
    """Función auxiliar genérica para leer cualquier tabla."""
    print(f"EXTRACT: Leyendo {table_name}")
    return pd.read_sql_table(table_name, co_oltp, schema=schema)

def extract_sales_territory(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'sales_territory')

def extract_currency(co_olt: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo Currency")

    df_currency = pd.read_sql(
        'SELECT * FROM sales.currency ORDER BY name ASC;',
        co_olt)

    return df_currency

def extract_geography(co_olt: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo Geography")

    query_geography = text("""
    SELECT DISTINCT
        a.city,
        a.postal_code,
        sp.state_province_code,
        sp.name AS state_province_name,
        sp.country_region_code,
        cr.name AS country_region_name,
        st.territory_id AS sales_territory_alternate_key
    FROM
        person.address AS a
        INNER JOIN person.state_province AS sp
            ON a.state_province_id = sp.state_province_id
        INNER JOIN person.country_region AS cr
            ON sp.country_region_code = cr.country_region_code
        INNER JOIN sales.sales_territory AS st
            ON sp.territory_id = st.territory_id
        INNER JOIN person.business_entity_address AS bea
            ON a.address_id = bea.address_id
        LEFT JOIN sales.customer AS c
            ON bea.business_entity_id = c.person_id -- Unir si la entidad es un cliente individual
        LEFT JOIN sales.store AS s
            ON bea.business_entity_id = s.business_entity_id -- Unir si la entidad es un revendedor (tienda)
    WHERE
        c.customer_id IS NOT NULL OR s.business_entity_id IS NOT NULL;
    """)

    return pd.read_sql(query_geography, co_olt)

def extract_promotion(co_otl: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_otl, schema, 'special_offer')

def extract_product_category(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product_category')

def extract_product_subcategory(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product_subcategory')

def extract_sales_order(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo sales order")
    return pd.read_sql(
        """
        SELECT 
        p.product_id,
        p.standard_cost,
        ROUND(AVG(sod.unit_price * (1 - sod.unit_price_discount)), 4) AS dealer_price
        FROM sales.sales_order_detail AS sod
        JOIN sales.sales_order_header AS soh 
            ON sod.sales_order_id = soh.sales_order_id
        JOIN sales.customer AS c 
            ON soh.customer_id = c.customer_id
        JOIN sales.store AS s 
            ON c.store_id = s.business_entity_id
        JOIN production.product AS p 
            ON sod.product_id = p.product_id
        GROUP BY p.product_id, p.standard_cost
        ORDER BY p.product_id;
        """,
        co_oltp)

def extract_product_model(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo product model")
    return pd.read_sql(
        "SELECT product_model_id, name FROM production.product_model",
        co_oltp)

def extract_large_photo(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo large photo")
    return pd.read_sql(
        """
        SELECT ppp.product_id, pp.large_photo                       
        FROM production.product_photo AS pp, production.product_product_photo AS ppp
        WHERE pp.product_photo_id=ppp.product_photo_id
        """,
        co_oltp)

def extract_product_price_list(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo price list")
    return pd.read_sql(
        """
        SELECT ph.product_id, ph.start_date, ph.end_date
        FROM production.product_list_price_history as ph 
        """,
        co_oltp)

def extract_language_description(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo language description")
    return pd.read_sql(
        """
        SELECT p.product_id,  pd.description ,pc.name
        FROM production.product as p JOIN production.product_model_prod_desc_culture as pmpdc
        ON p.product_model_id=pmpdc.product_model_id
        JOIN production.product_description as pd
        ON pmpdc.product_description_id=pd.product_description_id
        JOIN production.culture as pc
        ON pmpdc.culture_id=pc.culture_id
        """,
        co_oltp)

def extract_product(co_oltp: Engine, schema: str) -> pd.DataFrame:
    return _read_table(co_oltp, schema, 'product')

