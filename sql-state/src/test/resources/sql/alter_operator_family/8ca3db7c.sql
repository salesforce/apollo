-- file:alter_generic.sql ln:323 expect:true
ALTER OPERATOR FAMILY alt_nsp6.alt_opf6 USING btree ADD OPERATOR 1 < (int4, int2)
