SELECT e.national_idnumber, h.rate as base_rate
FROM hr.employee as e
         JOIN hr.employee_pay_history as h
              ON e.business_entity_id = h.business_entity_id
WHERE h.rate_change_date =
      (SELECT MAX(rate_change_date)
       FROM hr.employee_pay_history
       WHERE business_entity_id = e.business_entity_id)