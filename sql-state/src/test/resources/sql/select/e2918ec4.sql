-- file:json.sql ln:441 expect:true
SELECT ia1 FROM json_populate_record(NULL::jsrec, '{"ia1": [1, "2", null, 4]}') q
