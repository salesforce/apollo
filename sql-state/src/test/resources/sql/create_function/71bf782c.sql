-- file:create_operator.sql ln:164 expect:true
CREATE FUNCTION fn_op4(int8, type_op4)
RETURNS int8 AS $$
    SELECT NULL::int8
