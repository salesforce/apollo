-- file:xml.sql ln:596 expect:true
SELECT xmltable.* FROM xmltest2, LATERAL xmltable(('/d/r/' || lower(_path) || 'c') PASSING x COLUMNS a int PATH 'x' DEFAULT ascii(_path) - 54)
