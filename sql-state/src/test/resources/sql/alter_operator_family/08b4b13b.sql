-- file:alter_generic.sql ln:329 expect:true
ALTER OPERATOR FAMILY alt_opf7 USING btree DROP OPERATOR 1 (int4, int2, int8)
