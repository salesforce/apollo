-- file:jsonb.sql ln:578 expect:true
SELECT ia3 FROM jsonb_populate_record(NULL::jsbrec, '{"ia3": [1, "2", null, 4]}') q
