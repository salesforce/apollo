-- file:json.sql ln:445 expect:true
SELECT ia1d FROM json_populate_record(NULL::jsrec, '{"ia1d": 123}') q
