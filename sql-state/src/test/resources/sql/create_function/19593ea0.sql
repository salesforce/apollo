-- file:plpgsql.sql ln:147 expect:true
create function tg_room_ad() returns trigger as '
begin
    delete from WSlot where roomno = old.roomno
