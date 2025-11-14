SELECT p.first_name || ' ' || p.last_name AS emergency_contact_name,
       t1.phone_number                    AS emergency_contact_phone,
       e.national_idnumber
FROM hr.employee AS e
         JOIN
     person.person AS p ON e.business_entity_id = p.business_entity_id
         JOIN
     -- Subquery to rank and select the best phone number
         (SELECT pp.business_entity_id,
                 pp.phone_number,
                 -- Assign a rank based on the phone number type
                 ROW_NUMBER() OVER (
                        PARTITION BY pp.business_entity_id
                        ORDER BY
                            CASE pt.name
                                WHEN 'Work' THEN 1
                                WHEN 'Home' THEN 2
                                WHEN 'Cell' THEN 3
                                ELSE 4
                            END
                    ) AS rn
          FROM person.person_phone AS pp
                   JOIN
               person.phone_number_type AS pt ON pt.phone_number_type_id = pp.phone_number_type_id) AS t1
     ON t1.business_entity_id = e.business_entity_id
WHERE t1.rn = 1;