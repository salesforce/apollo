-- file:json.sql ln:459 expect:true
SELECT ia3 FROM json_populate_record(NULL::jsrec, '{"ia3": [[1, 2], [null, 4]]}') q
