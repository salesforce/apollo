-- file:json.sql ln:461 expect:true
SELECT ia3 FROM json_populate_record(NULL::jsrec, '{"ia3": [ [[1, 2]], [[3, 4]] ]}') q
