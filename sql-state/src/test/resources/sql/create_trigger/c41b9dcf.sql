-- file:plpgsql.sql ln:849 expect:true
create trigger tg_slotlink_a after insert or update or delete
    on PHone for each row execute procedure tg_slotlink_a('PH')
