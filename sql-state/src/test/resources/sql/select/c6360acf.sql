-- file:rangetypes.sql ln:167 expect:true
select daterange('2000-01-10'::date, '2000-01-11'::date, '(]')
