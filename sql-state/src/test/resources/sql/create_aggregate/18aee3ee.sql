-- file:object_address.sql ln:30 expect:true
CREATE AGGREGATE addr_nsp.genaggr(int4) (sfunc = int4pl, stype = int4)
