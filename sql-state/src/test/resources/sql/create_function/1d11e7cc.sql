-- file:plpgsql.sql ln:232 expect:true
create function tg_system_au() returns trigger as '
begin
    if new.name != old.name then
        update IFace set sysname = new.name where sysname = old.name
