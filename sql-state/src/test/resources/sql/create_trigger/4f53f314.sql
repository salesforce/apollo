-- file:triggers.sql ln:1856 expect:true
create trigger self_ref_before_trig
  before delete on self_ref
  for each statement execute procedure trigger_func('self_ref')
