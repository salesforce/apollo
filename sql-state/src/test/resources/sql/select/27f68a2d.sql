-- file:xml.sql ln:58 expect:true
SELECT xmlelement(name foo, xmlattributes('2009-04-09 00:24:37'::timestamp as bar))
