SELECT p.product_id,
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