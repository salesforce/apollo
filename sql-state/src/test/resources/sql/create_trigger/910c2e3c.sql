-- file:triggers.sql ln:137 expect:true
create trigger trigger_return_old
	before insert or delete or update on trigtest
	for each row execute procedure trigger_return_old()
