-- file:jsonb.sql ln:572 expect:true
SELECT ia2 FROM jsonb_populate_record(NULL::jsbrec, '{"ia2": [[1, 2], [3]]}') q
