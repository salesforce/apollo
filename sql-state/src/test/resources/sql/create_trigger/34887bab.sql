-- file:plpgsql.sql ln:691 expect:true
create trigger tg_backlink_a after insert or update or delete
    on PLine for each row execute procedure tg_backlink_a('PL')
