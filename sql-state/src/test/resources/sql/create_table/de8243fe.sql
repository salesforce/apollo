-- file:plpgsql.sql ln:61 expect:true
create table PLine (
    slotname	char(20),
    phonenumber	char(20),
    comment	text,
    backlink	char(20)
)
