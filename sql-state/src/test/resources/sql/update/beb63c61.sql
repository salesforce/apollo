-- file:returning.sql ln:120 expect:true
update voo set f1 = f1 + 1 where f2 = 'zoo2' RETURNING *, f1*2
