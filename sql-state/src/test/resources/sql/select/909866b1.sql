-- file:text.sql ln:36 expect:true
select concat(1,2,3,'hello',true, false, to_date('20100309','YYYYMMDD'))
