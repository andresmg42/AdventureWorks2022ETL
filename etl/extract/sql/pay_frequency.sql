SELECT E.national_idnumber,
       LatestPay.pay_frequency
FROM hr.employee AS E
         -- LEFT JOIN LATERAL is the exact equivalent of OUTER APPLY
         LEFT JOIN LATERAL (
    SELECT PH.pay_frequency
    FROM hr.employee_pay_history AS PH
    WHERE PH.business_entity_id = E.business_entity_id
    ORDER BY PH.rate_change_date DESC
        LIMIT 1
        ) AS LatestPay
ON true;