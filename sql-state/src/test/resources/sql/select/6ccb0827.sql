-- file:jsonb.sql ln:575 expect:true
SELECT ia2d FROM jsonb_populate_record(NULL::jsbrec, '{"ia2d": [[1, "2"], [null, 4]]}') q
