-- file:rangetypes.sql ln:425 expect:true
select rangetypes_sql(numrange(1,10), ARRAY[2,20])
