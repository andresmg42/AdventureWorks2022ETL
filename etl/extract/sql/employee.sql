SELECT e.business_entity_id AS employee_alternate_key,
       p.title,
       p.first_name,
       p.middle_name,
       p.last_name,
       p.suffix,
       e.gender,
       e.marital_status,
       e.birth_date,
       e.hire_date,
       e.salaried_flag,
       e.vacation_hours,
       e.sick_leave_hours,
       e.current_flag,
       e.organization_level,
       e.job_title,
       e.login_id,
       ea.email_address,
       e.national_idnumber  AS employee_national_id_alternate_key,
       pp.phone_number      AS phone,
       d.name               AS department_name,
       h.start_date,
       h.end_date
FROM hr.employee AS e
         INNER JOIN person.person AS p
                    ON e.business_entity_id = p.business_entity_id
         LEFT JOIN person.email_address AS ea
                   ON p.business_entity_id = ea.business_entity_id
         LEFT JOIN person.person_phone AS pp
                   ON p.business_entity_id = pp.business_entity_id
         LEFT JOIN hr.employee_department_history AS h
                   ON e.business_entity_id = h.business_entity_id
         LEFT JOIN hr.department AS d
                   ON h.department_id = d.department_id