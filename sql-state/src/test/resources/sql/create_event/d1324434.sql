-- file:event_trigger.sql ln:59 expect:true
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('CREATE DATABASE')
   execute procedure test_event_trigger()
