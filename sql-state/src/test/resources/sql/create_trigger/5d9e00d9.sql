-- file:triggers.sql ln:408 expect:true
create trigger trigtest_b_stmt_tg before insert or update or delete on trigtest
for each statement execute procedure trigtest()
