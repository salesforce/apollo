-- file:misc_functions.sql ln:18 expect:true
SELECT num_nulls(1, 2, NULL::text, NULL::point, '', int8 '9', 1.0 / NULL)
