-- file:jsonb.sql ln:553 expect:true
SELECT ia FROM jsonb_populate_record(NULL::jsbrec, '{"ia": [1, "2", null, 4]}') q
