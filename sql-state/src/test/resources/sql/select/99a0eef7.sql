-- file:jsonb.sql ln:581 expect:true
SELECT ia3 FROM jsonb_populate_record(NULL::jsbrec, '{"ia3": [ [[1, 2]], [[3, 4]] ]}') q
