-- file:join.sql ln:1513 expect:true
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1
