-- file:subselect.sql ln:113 expect:true
explain (costs off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit null)
