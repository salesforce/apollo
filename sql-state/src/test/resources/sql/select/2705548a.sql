-- file:json.sql ln:463 expect:true
SELECT ia3 FROM json_populate_record(NULL::jsrec, '{"ia3": [ [[1, 2], [3, 4]], [[5, 6], [7, 8], [9, 10]] ]}') q
