-- file:rangetypes.sql ln:99 expect:true
select numrange(4.5, 5.5, '[]') && numrange(5.5, 6.5)
