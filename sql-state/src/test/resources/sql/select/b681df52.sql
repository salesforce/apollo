-- file:jsonb.sql ln:652 expect:true
select * from jsonb_populate_recordset(row(0::int),'[{"a":"1","b":"2"},{"a":"3"}]') q (a text, b text)
