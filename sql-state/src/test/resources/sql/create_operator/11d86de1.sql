-- file:equivclass.sql ln:67 expect:true
create operator = (
    procedure = int8alias1eq,
    leftarg = int8, rightarg = int8alias1,
    restrict = eqsel, join = eqjoinsel,
    merges
)
