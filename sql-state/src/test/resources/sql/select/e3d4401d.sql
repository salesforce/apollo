-- file:jsonb.sql ln:662 expect:true
select * from jsonb_to_recordset('[{"a":1,"b":"foo","d":false},{"a":2,"b":"bar","c":true}]')
    as x(a int, b text, c boolean)
