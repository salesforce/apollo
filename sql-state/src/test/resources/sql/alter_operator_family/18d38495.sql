-- file:alter_generic.sql ln:298 expect:true
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD OPERATOR 6 < (int4, int2)
