-- CTE para mapear sales_person_id
WITH EmployeeMapping AS (SELECT e.business_entity_id,
                                e.national_idnumber AS employee_national_id
                         FROM hr.employee AS e),
     -- CTE para obtener el account_number por store_id
     ResellerAccountMapping AS (SELECT c.store_id,
                                       MIN(c.account_number) AS account_number
                                FROM sales.customer AS c
                                WHERE c.store_id IS NOT NULL
                                GROUP BY c.store_id)
SELECT sod.sales_order_id,
       soh.sales_order_number,
       sod.sales_order_detail_id,
       ROW_NUMBER()                                                        OVER (PARTITION BY soh.sales_order_number ORDER BY sod.sales_order_detail_id) AS sales_order_line_number, sod.order_qty as order_quantity,
       sod.unit_price,
       (sod.unit_price * sod.order_qty)                                 AS extended_amount,
       sod.unit_price_discount                                          as unit_price_discount_pct,
       (sod.unit_price * sod.unit_price_discount * sod.order_qty)       AS discount_amount,
       p.product_number,
       p.standard_cost                                                  AS product_standard_cost,
       (sod.order_qty * p.standard_cost)                                AS total_product_cost,
       (sod.unit_price * (1 - sod.unit_price_discount) * sod.order_qty) AS sales_amount,
       soh.tax_amt,
       soh.freight,
       soh.order_date,
       soh.due_date,
       soh.ship_date,
       soh.territory_id,
       soh.sales_person_id,
       em.employee_national_id,
       soh.currency_rate_id,
       soh.purchase_order_number                                        AS customer_po_number,
       soh.revision_number,
       sod.carrier_tracking_number,
       r.business_entity_id                                             AS reseller_id,
       ram.account_number                                               AS reseller_account_number
FROM sales.sales_order_header AS soh
         JOIN sales.sales_order_detail AS sod
              ON soh.sales_order_id = sod.sales_order_id
         JOIN production.product AS p
              ON sod.product_id = p.product_id
         JOIN sales.customer AS c
              ON soh.customer_id = c.customer_id
         JOIN sales.store AS r
              ON c.store_id = r.business_entity_id
         LEFT JOIN ResellerAccountMapping AS ram
                   ON r.business_entity_id = ram.store_id
         LEFT JOIN EmployeeMapping AS em
                   ON soh.sales_person_id = em.business_entity_id
WHERE soh.online_order_flag = false
  AND soh.sales_person_id IS NOT NULL;