-- file:domain.sql ln:35 expect:true
create table basictest
           ( testint4 domainint4
           , testtext domaintext
           , testvarchar domainvarchar
           , testnumeric domainnumeric
           )
