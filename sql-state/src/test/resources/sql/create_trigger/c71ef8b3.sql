-- file:plpgsql.sql ln:837 expect:true
create trigger tg_slotlink_a after insert or update or delete
    on PSlot for each row execute procedure tg_slotlink_a('PS')
