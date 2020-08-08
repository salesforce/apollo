-- file:triggers.sql ln:1617 expect:true
create trigger parent_delete_trig
  after delete on parent referencing old table as old_table
  for each statement execute procedure dump_delete()
