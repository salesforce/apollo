-- file:rangetypes.sql ln:163 expect:true
select daterange('2000-01-10'::date, '2000-01-20'::date, '[)')
