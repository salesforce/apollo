-- file:arrays.sql ln:520 expect:true
select a, a = '{}' as is_eq, array_dims(a)
  from (select array_fill(42, '{}') as a) ss
