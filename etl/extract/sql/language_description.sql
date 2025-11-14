SELECT p.product_id, pd.description, pc.name
FROM production.product as p
         JOIN production.product_model_prod_desc_culture as pmpdc
              ON p.product_model_id = pmpdc.product_model_id
         JOIN production.product_description as pd
              ON pmpdc.product_description_id = pd.product_description_id
         JOIN production.culture as pc
              ON pmpdc.culture_id = pc.culture_id