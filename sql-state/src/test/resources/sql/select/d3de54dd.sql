-- file:xml.sql ln:44 expect:true
SELECT xmlelement(name duplicate, xmlattributes(1 as a, 2 as b, 3 as a))
