-- file:triggers.sql ln:1634 expect:true
create trigger child2_update_trig
  after update on child2 referencing old table as old_table new table as new_table
  for each statement execute procedure dump_update()
