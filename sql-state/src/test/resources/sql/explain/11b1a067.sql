-- file:inherit.sql ln:699 expect:true
explain (costs off) select * from pp_arrpart where a in ('{4, 5}', '{1}')
