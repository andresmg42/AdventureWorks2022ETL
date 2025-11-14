WITH internet_customers AS (SELECT DISTINCT customer_id
                            FROM sales.sales_order_header
                            WHERE online_order_flag = true)

SELECT c.account_number AS customer_alternate_key,
       p.title,
       p.first_name,
       p.middle_name,
       p.last_name,
       p.suffix,
       p.name_style,
       p.demographics,
       e.email_address,
       rp.phone_number  AS phone,
       a.address_line_1 AS address_line1,
       a.address_line_2 AS address_line2,

       a.city,
       a.postal_code,
       sp.state_province_code,
       cr.country_region_code
FROM sales.customer AS c
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