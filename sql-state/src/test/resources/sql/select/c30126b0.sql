-- file:timestamptz.sql ln:458 expect:true
select * from tmptz where f1 at time zone 'utc' = '2017-01-18 00:00'
