-- file:arrays.sql ln:480 expect:true
insert into comptable
  values (row(1,'foo'), array[row(2,'bar')::comptype, row(3,'baz')::comptype])
