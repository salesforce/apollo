-- file:rangefuncs.sql ln:621 expect:true
select x from int8_tbl, extractq2_2(int8_tbl) f(x)
