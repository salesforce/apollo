-- file:triggers.sql ln:708 expect:true
CREATE TRIGGER invalid_trig INSTEAD OF INSERT ON main_table
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_ins')
