-- file:jsonb.sql ln:899 expect:true
select jsonb_strip_nulls('[1,{"a":1,"b":null,"c":2},3]')
