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

def extract_customer(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo customer")
    return pd.read_sql(
        """
        WITH internet_customers AS (
            SELECT DISTINCT customer_id
            FROM sales.sales_order_header
            WHERE online_order_flag = true
        )
        
        SELECT
            c.account_number AS customer_alternate_key,
            p.title,
            p.first_name,
            p.middle_name,
            p.last_name,
            p.suffix,
            p.name_style,
            p.demographics,
            e.email_address,
            rp.phone_number AS phone,
            a.address_line_1 AS address_line1,
            a.address_line_2 AS address_line2,
        
            a.city,
            a.postal_code,
            sp.state_province_code,
            cr.country_region_code
        FROM
            sales.customer AS c
            INNER JOIN internet_customers AS ic
                ON c.customer_id = ic.customer_id
            INNER JOIN person.person AS p
                ON c.person_id = p.business_entity_id
            INNER JOIN person.email_address AS e
                ON p.business_entity_id = e.business_entity_id
            LEFT JOIN person.person_phone AS rp
                ON p.business_entity_id = rp.business_entity_id
            INNER JOIN person.business_entity_address AS bea
                ON p.business_entity_id = bea.business_entity_id
            INNER JOIN person.address AS a
                ON bea.address_id = a.address_id
            INNER JOIN person.state_province AS sp
                ON a.state_province_id = sp.state_province_id
            INNER JOIN person.country_region AS cr
                ON sp.country_region_code = cr.country_region_code
            INNER JOIN person.address_type AS at
                ON bea.address_type_id = at.address_type_id
        WHERE
            at.name = 'Home';
        """,
        co_oltp)

def extract_emergency_contact_data(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo contact table")
    return pd.read_sql(
        """
        SELECT
            p.first_name || ' ' || p.last_name AS emergency_contact_name,
            t1.phone_number AS emergency_contact_phone,
            e.national_idnumber 
        FROM
            hr.employee AS e
        JOIN
            person.person AS p ON e.business_entity_id = p.business_entity_id
        JOIN
            -- Subquery to rank and select the best phone number
            (
                SELECT
                    pp.business_entity_id,
                    pp.phone_number,
                    -- Assign a rank based on the phone number type
                    ROW_NUMBER() OVER (
                        PARTITION BY pp.business_entity_id 
                        ORDER BY
                            CASE pt.name
                                WHEN 'Work' THEN 1  
                                WHEN 'Home' THEN 2
                                WHEN 'Cell' THEN 3  
                                ELSE 4
                            END
                    ) AS rn
                FROM
                    person.person_phone AS pp
                JOIN
                    person.phone_number_type AS pt ON pt.phone_number_type_id = pp.phone_number_type_id
            ) AS t1 ON t1.business_entity_id = e.business_entity_id
        WHERE
            t1.rn = 1;
        """,
        co_oltp)

def extract_sales_person(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo sales person")
    return pd.read_sql(
        text("SELECT business_entity_id AS employee_alternate_key, territory_id FROM sales.sales_person;"),
        co_oltp
    )

def extract_pay_frequency(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo pay frequency")
    return pd.read_sql(
        """
        SELECT
            E.national_idnumber,
            LatestPay.pay_frequency
        FROM
            hr.employee AS E
        -- LEFT JOIN LATERAL is the exact equivalent of OUTER APPLY
        LEFT JOIN LATERAL (
            SELECT
                PH.pay_frequency
            FROM
                hr.employee_pay_history AS PH
            WHERE
                PH.business_entity_id = E.business_entity_id
            ORDER BY
                PH.rate_change_date DESC
            LIMIT 1 
        ) AS LatestPay ON true;
        """,
        co_oltp)

def extract_base_rate(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo base rate")
    return pd.read_sql(
        """
        SELECT e.national_idnumber,h.rate as base_rate
        FROM hr.employee as e
            JOIN hr.employee_pay_history as h
            ON e.business_entity_id=h.business_entity_id
            WHERE h.rate_change_date=
            (
                SELECT MAX(rate_change_date)
                FROM hr.employee_pay_history
                WHERE business_entity_id=e.business_entity_id
            )
        """,
        co_oltp)


def extract_employee(co_otlp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo employee")
    return pd.read_sql(
        """
        SELECT
            e.business_entity_id AS employee_alternate_key,
            p.title,
            p.first_name,
            p.middle_name,
            p.last_name,
            p.suffix,
            e.gender,
            e.marital_status,
            e.birth_date,
            e.hire_date,
            e.salaried_flag,
            e.vacation_hours,
            e.sick_leave_hours,
            e.current_flag,
            e.organization_level,
            e.job_title,
            e.login_id,
            ea.email_address,
            e.national_idnumber as employee_national_id_alternate_key,
            pp.phone_number AS phone,
            d.name AS department_name,
            h.start_date,
            h.end_date
        FROM hr.employee AS e
        INNER JOIN person.person AS p
            ON e.business_entity_id = p.business_entity_id
        LEFT JOIN person.email_address AS ea
            ON p.business_entity_id = ea.business_entity_id
        LEFT JOIN person.person_phone AS pp
            ON p.business_entity_id = pp.business_entity_id
        LEFT JOIN hr.employee_department_history AS h
            ON e.business_entity_id = h.business_entity_id
        LEFT JOIN hr.department AS d
            ON h.department_id = d.department_id
        """,
        co_otlp)

def extract_reseller(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo reseller")
    return pd.read_sql(
        """
        -- CTE para pre-agregar información de clientes y órdenes
        WITH ResellerCustomerInfo AS (
            SELECT
                c.store_id,
                MIN(c.account_number) AS reseller_alternate_key,
                EXTRACT(YEAR FROM MIN(soh.order_date)) AS first_order_year,
                EXTRACT(YEAR FROM MAX(soh.order_date)) AS last_order_year,
                EXTRACT(MONTH FROM MAX(soh.order_date)) AS order_month,
                '0' as order_frequency
            FROM sales.customer AS c
            LEFT JOIN sales.sales_order_header AS soh ON c.customer_id = soh.customer_id
            WHERE c.store_id IS NOT NULL
            GROUP BY c.store_id
        ),
        -- CTE para seleccionar una única dirección por tienda (Revendedor)
        RankedAddresses AS (
            SELECT
                bea.business_entity_id,
                a.address_line_1,
                a.address_line_2,
                a.city,
                a.postal_code,
                st.name AS state_province_name,
                cr.name AS country_region_name,
                ROW_NUMBER() OVER(
                    PARTITION BY bea.business_entity_id
                    ORDER BY
                        CASE
                            WHEN at.name = 'Main Office' THEN 1
                            ELSE 99
                        END
                ) as rn_addr
            FROM person.business_entity_address AS bea
            JOIN person.address AS a ON bea.address_id = a.address_id
            JOIN person.address_type AS at ON bea.address_type_id = at.address_type_id
            JOIN person.state_province AS st ON a.state_province_id = st.state_province_id
            JOIN person.country_region AS cr ON st.country_region_code = cr.country_region_code
        ),
        -- CTE para seleccionar un unico teléfono por tienda, a través del contacto
        RankedContacts AS (
            SELECT
                bec.business_entity_id,
                pp.phone_number,
                ROW_NUMBER() OVER(
                    PARTITION BY bec.business_entity_id
                    ORDER BY bec.contact_type_id
                ) as rn_contact
            FROM person.business_entity_contact AS bec
            JOIN person.person AS p ON bec.person_id = p.business_entity_id
            JOIN person.person_phone AS pp ON p.business_entity_id = pp.business_entity_id
        )
        SELECT
            s.business_entity_id,
            cus.account_number AS reseller_alternate_key,
            s.name AS reseller_name,
            s.demographics,
        
            -- Datos de la dirección desde la CTE de direcciones
            ra.address_line_1 AS address_line1,
            ra.address_line_2 AS address_line2,
            ra.city,
            ra.postal_code,
            ra.state_province_name,
            ra.country_region_name,
        
            -- Datos del cliente pre-agregados desde la primera CTE
            rci.first_order_year,
            rci.last_order_year,
            rci.order_month,
            rci.order_frequency,
        
            -- El teléfono desde la CTE de contactos
            rc.phone_number AS phone
        
        FROM sales.store AS s
        
        LEFT JOIN ResellerCustomerInfo AS rci ON s.business_entity_id = rci.store_id
        
        LEFT JOIN RankedAddresses AS ra
            ON s.business_entity_id = ra.business_entity_id
            AND ra.rn_addr = 1
        
        LEFT JOIN RankedContacts AS rc
            ON s.business_entity_id = rc.business_entity_id
            AND rc.rn_contact = 1
        
        LEFT JOIN sales.customer as cus
            ON cus.store_id = s.business_entity_id
            AND cus.person_id IS NULL;
        """,
        co_oltp)

def extract_special_offer(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo special offer")
    return pd.read_sql(
        """
        SELECT
            p.product_number, sod.special_offer_id
        FROM
            sales.sales_order_detail AS sod
            JOIN production.product AS p
              ON sod.product_id = p.product_id
            JOIN sales.sales_order_header AS soh
              ON sod.sales_order_id = soh.sales_order_id
            WHERE soh.online_order_flag = true
        """,
        co_oltp)

def extract_fact_internet_sales(co_oltp: Engine) -> pd.DataFrame:
    print("EXTRACT: Leyendo fact internet sales")
    return pd.read_sql(
        """
        SELECT
            p.product_number, 
            sh.order_date, 
            sh.due_date, 
            sh.ship_date,
            sh.territory_id,
            sh.sales_order_number,
            sh.revision_number,
            cu.account_number,
            COALESCE(cr.to_currency_code, 'USD') AS to_currency_code,
            ROW_NUMBER() OVER (PARTITION BY sh.sales_order_number ORDER BY so.sales_order_detail_id) AS sales_order_line_number,
            so.order_qty AS order_quantity,
            so.unit_price,
            so.unit_price_discount AS unit_price_discount_pct,
            (so.unit_price * so.order_qty) AS extended_amount,
            (so.unit_price * so.unit_price_discount * so.order_qty) AS discount_amount,
            p.standard_cost AS product_standard_cost,
            (so.order_qty * p.standard_cost) AS total_product_cost,
            (so.unit_price *(1-so.unit_price_discount)*so.order_qty) AS sales_amount,
            sh.tax_amt,
            sh.freight,
            so.carrier_tracking_number,
            sh.purchase_order_number AS customer_po_number                     
        FROM
            sales.sales_order_header as sh
            JOIN sales.sales_order_detail as so
            ON sh.sales_order_id=so.sales_order_id
            JOIN production.product as p
            ON so.product_id=p.product_id
            LEFT JOIN sales.currency_rate as cr
            ON sh.currency_rate_id=cr.currency_rate_id
            JOIN sales.customer as cu
            ON sh.customer_id=cu.customer_id
            WHERE sh.online_order_flag=true
        """,
        co_oltp)
