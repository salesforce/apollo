-- file:plpgsql.sql ln:4544 expect:true
CREATE TRIGGER transition_table_base_upd_trig
  AFTER UPDATE ON transition_table_base
  REFERENCING OLD TABLE AS oldtable NEW TABLE AS newtable
  FOR EACH STATEMENT
  EXECUTE PROCEDURE transition_table_base_upd_func()
