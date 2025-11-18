SELECT geography_key,
       city,
       postal_code,
       state_province_code,
       state_province_name,
       country_region_code,
       english_country_region_name AS country_region_name
FROM dw.dim_geography;