-- file:jsonb.sql ln:655 expect:true
select * from jsonb_populate_recordset(row(1000000000::int,50::int),'[{"b":"2"},{"a":"3"}]') q (a text, b text)
