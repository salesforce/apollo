-- file:triggers.sql ln:1331 expect:true
create trigger trig_upd_before before update on parted_stmt_trig
  for each statement execute procedure trigger_notice()
