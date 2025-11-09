import pandas as pd
from sqlalchemy import text, Engine

def extract_sales_territory(co_olt: Engine, schema) -> pd.DataFrame:
    print("EXTRACT: Leyendo SalesTerritory")

    return pd.read_sql_table('sales_territory',co_olt, schema=schema)

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

def extract_promotion(co_otl: Engine, schema) -> pd.DataFrame:
    print("EXTRACT: Leyendo promotion")
    return pd.read_sql_table('special_offer', co_otl, schema=schema)

def extract_product_category(co_oltp: Engine, schema) -> pd.DataFrame:
    print("EXTRACT: Leyendo product category")
    return pd.read_sql_table('product_category', co_oltp, schema=schema)

def extract_product_subcategory(co_oltp: Engine, schema) -> pd.DataFrame:
    print("EXTRACT: Leyendo product subcategory")
    return pd.read_sql_table('product_subcategory', co_oltp, schema=schema)
