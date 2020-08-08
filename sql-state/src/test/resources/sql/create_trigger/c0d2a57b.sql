-- file:plpgsql.sql ln:4513 expect:true
CREATE TRIGGER transition_table_base_ins_trig
  AFTER INSERT ON transition_table_base
  REFERENCING NEW TABLE AS newtable
  FOR EACH STATEMENT
  EXECUTE PROCEDURE transition_table_base_ins_func()
