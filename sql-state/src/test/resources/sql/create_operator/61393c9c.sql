-- file:equivclass.sql ln:55 expect:true
create operator = (
    procedure = int8alias2eq,
    leftarg = int8alias2, rightarg = int8alias2,
    commutator = =,
    restrict = eqsel, join = eqjoinsel,
    merges
)
