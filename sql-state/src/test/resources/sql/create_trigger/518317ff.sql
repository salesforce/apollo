-- file:plpgsql.sql ln:528 expect:true
create trigger tg_wslot_bu before update
    on WSlot for each row execute procedure tg_Wslot_bu()
