-- file:triggers.sql ln:1545 expect:true
create trigger intercept_insert_child3
  before insert on child3
  for each row execute procedure intercept_insert()
