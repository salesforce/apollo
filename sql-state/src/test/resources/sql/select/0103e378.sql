-- file:interval.sql ln:296 expect:true
select interval_hash('30 days'::interval) = interval_hash('1 month'::interval) as t
