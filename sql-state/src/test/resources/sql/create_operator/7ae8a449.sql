-- file:equivclass.sql ln:89 expect:true
create operator < (
    procedure = int8alias1lt,
    leftarg = int8alias1, rightarg = int8alias1
)
