-- file:plpgsql.sql ln:840 expect:true
create trigger tg_slotlink_a after insert or update or delete
    on WSlot for each row execute procedure tg_slotlink_a('WS')
