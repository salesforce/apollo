-- file:subselect.sql ln:442 expect:true
select '1'::text in (select '1'::name union all select '1'::name)
