-- file:subselect.sql ln:521 expect:true
select * from
  (select distinct ten from tenk1) ss
  where ten < 10 + nextval('ts1')
  order by 1
