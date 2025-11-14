SELECT DISTINCT a.city,
                a.postal_code,
                sp.state_province_code,
                sp.name         AS state_province_name,
                sp.country_region_code,
                cr.name         AS country_region_name,
                st.territory_id AS sales_territory_alternate_key
FROM person.address AS a
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
WHERE c.customer_id IS NOT NULL
   OR s.business_entity_id IS NOT NULL;