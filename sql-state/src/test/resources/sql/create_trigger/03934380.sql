-- file:plpgsql.sql ln:499 expect:true
create trigger tg_pslot_bu before update
    on PSlot for each row execute procedure tg_pslot_bu()
