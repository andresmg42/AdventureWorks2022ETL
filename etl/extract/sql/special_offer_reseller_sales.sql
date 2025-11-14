SELECT p.product_number, sod.special_offer_id
FROM sales.sales_order_detail AS sod
         JOIN production.product AS p ON sod.product_id = p.product_id