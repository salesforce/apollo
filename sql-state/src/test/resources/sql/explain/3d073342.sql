-- file:equivclass.sql ln:119 expect:true
explain (costs off)
  select * from ec1 where ff = f1 and f1 = '42'::int8alias1
