-- file:triggers.sql ln:1335 expect:true
create trigger trig_del_before before delete on parted_stmt_trig
  for each statement execute procedure trigger_notice()
