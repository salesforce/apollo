-- file:json.sql ln:149 expect:true
SELECT row_to_json(q)
FROM (SELECT 'NaN'::float8 AS "float8field") q
