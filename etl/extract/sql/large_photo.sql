SELECT ppp.product_id, pp.large_photo
FROM production.product_photo AS pp,
     production.product_product_photo AS ppp
WHERE pp.product_photo_id = ppp.product_photo_id