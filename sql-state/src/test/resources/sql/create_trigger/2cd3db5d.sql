-- file:triggers.sql ln:1217 expect:true
create trigger trigger_ddl_func before insert on trigger_ddl_table for each row
  execute procedure trigger_ddl_func()
