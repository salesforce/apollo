-- file:triggers.sql ln:296 expect:true
CREATE TRIGGER delete_when AFTER DELETE ON main_table
FOR EACH STATEMENT WHEN (true) EXECUTE PROCEDURE trigger_func('delete_when')
