SELECT p.product_number,
       sh.order_date,
       sh.due_date,
       sh.ship_date,
       sh.territory_id,
       sh.sales_order_number,
       sh.revision_number,
       cu.account_number,
       COALESCE(cr.to_currency_code, 'USD')                          AS to_currency_code,
       ROW_NUMBER()                                                     OVER (PARTITION BY sh.sales_order_number ORDER BY so.sales_order_detail_id) AS sales_order_line_number, so.order_qty AS order_quantity,
       so.unit_price,
       so.unit_price_discount                                        AS unit_price_discount_pct,
       (so.unit_price * so.order_qty)                                AS extended_amount,
       (so.unit_price * so.unit_price_discount * so.order_qty)       AS discount_amount,
       p.standard_cost                                               AS product_standard_cost,
       (so.order_qty * p.standard_cost)                              AS total_product_cost,
       (so.unit_price * (1 - so.unit_price_discount) * so.order_qty) AS sales_amount,
       sh.tax_amt,
       sh.freight,
       so.carrier_tracking_number,
       sh.purchase_order_number                                      AS customer_po_number
FROM sales.sales_order_header as sh
         JOIN sales.sales_order_detail as so
              ON sh.sales_order_id = so.sales_order_id
         JOIN production.product as p
              ON so.product_id = p.product_id
         LEFT JOIN sales.currency_rate as cr
                   ON sh.currency_rate_id = cr.currency_rate_id
         JOIN sales.customer as cu
              ON sh.customer_id = cu.customer_id
WHERE sh.online_order_flag = true