-- file:triggers.sql ln:1329 expect:true
create trigger trig_ins_after after insert on parted_stmt_trig
  for each statement execute procedure trigger_notice()
