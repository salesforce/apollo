-- file:rangetypes.sql ln:166 expect:true
select daterange('2000-01-10'::date, '2000-01-11'::date, '()')
