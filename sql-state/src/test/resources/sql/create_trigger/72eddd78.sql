-- file:plpgsql.sql ln:688 expect:true
create trigger tg_backlink_a after insert or update or delete
    on WSlot for each row execute procedure tg_backlink_a('WS')
