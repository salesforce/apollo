-- file:triggers.sql ln:410 expect:true
create trigger trigtest_a_stmt_tg after insert or update or delete on trigtest
for each statement execute procedure trigtest()
