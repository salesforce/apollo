-- file:arrays.sql ln:189 expect:true
update arrtest1 set i[15:16] = array[null,26], t[15:16] = array[null,'p26']
