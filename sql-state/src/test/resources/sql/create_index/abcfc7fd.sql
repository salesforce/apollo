-- file:amutils.sql ln:79 expect:true
CREATE INDEX fooindex ON foo (f1 desc, f2 asc, f3 nulls first, f4 nulls last)
