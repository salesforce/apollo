-- file:triggers.sql ln:1257 expect:true
create trigger upsert_before_trig before insert or update on upsert
  for each row execute procedure upsert_before_func()
