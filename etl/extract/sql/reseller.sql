-- CTE para pre-agregar información de clientes y órdenes
WITH ResellerCustomerInfo AS (SELECT c.store_id,
                                     MIN(c.account_number)                   AS reseller_alternate_key,
                                     EXTRACT(YEAR FROM MIN(soh.order_date))  AS first_order_year,
                                     EXTRACT(YEAR FROM MAX(soh.order_date))  AS last_order_year,
                                     EXTRACT(MONTH FROM MAX(soh.order_date)) AS order_month,
                                     '0'                                     as order_frequency
                              FROM sales.customer AS c
                                       LEFT JOIN sales.sales_order_header AS soh ON c.customer_id = soh.customer_id
                              WHERE c.store_id IS NOT NULL
                              GROUP BY c.store_id),
     -- CTE para seleccionar una única dirección por tienda (Revendedor)
     RankedAddresses AS (SELECT bea.business_entity_id,
                                a.address_line_1,
                                a.address_line_2,
                                a.city,
                                a.postal_code,
                                st.name AS   state_province_name,
                                cr.name AS   country_region_name,
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
                                  JOIN person.address_type AS at
ON bea.address_type_id = at.address_type_id
    JOIN person.state_province AS st ON a.state_province_id = st.state_province_id
    JOIN person.country_region AS cr ON st.country_region_code = cr.country_region_code
    ),
    -- CTE para seleccionar un unico teléfono por tienda, a través del contacto
    RankedContacts AS (
SELECT
    bec.business_entity_id, pp.phone_number, ROW_NUMBER() OVER(
    PARTITION BY bec.business_entity_id
    ORDER BY bec.contact_type_id
    ) as rn_contact
FROM person.business_entity_contact AS bec
    JOIN person.person AS p
ON bec.person_id = p.business_entity_id
    JOIN person.person_phone AS pp ON p.business_entity_id = pp.business_entity_id
    )
SELECT s.business_entity_id,
       cus.account_number AS reseller_alternate_key,
       s.name             AS reseller_name,
       s.demographics,

       -- Datos de la dirección desde la CTE de direcciones
       ra.address_line_1  AS address_line1,
       ra.address_line_2  AS address_line2,
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
       rc.phone_number    AS phone

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