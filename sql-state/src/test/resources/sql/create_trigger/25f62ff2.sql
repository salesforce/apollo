-- file:insert.sql ln:231 expect:true
create trigger mlparted11_trig before insert ON mlparted11
  for each row execute procedure mlparted11_trig_fn()
