-- file:text.sql ln:38 expect:true
select concat_ws('#',1,2,3,'hello',true, false, to_date('20100309','YYYYMMDD'))
