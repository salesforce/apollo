-- file:event_trigger.sql ln:54 expect:true
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('CREATE ROLE')
   execute procedure test_event_trigger()
