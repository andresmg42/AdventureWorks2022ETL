SELECT p.product_number,
       sod.special_offer_id
FROM sales.sales_order_detail AS sod
         JOIN production.product AS p
              ON sod.product_id = p.product_id
         JOIN sales.sales_order_header AS soh
              ON sod.sales_order_id = soh.sales_order_id
WHERE soh.online_order_flag = true