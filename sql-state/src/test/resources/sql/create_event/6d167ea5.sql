-- file:event_trigger.sql ln:78 expect:true
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('create table', 'CREATE FUNCTION')
   execute procedure test_event_trigger()
