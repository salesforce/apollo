-- file:polymorphism.sql ln:393 expect:true
select q2, sql_if(q2 > 0, q2, q2 + 1) from int8_tbl
