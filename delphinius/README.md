# Delphinius
A DataLog to SQL translation and dynamic evoluton engine
___
This module provides a reasonably generic Datalog query and schema definition functionality.  Delphinius uses ANTLR4 for the Datalog AST and JOOQ as the type safe intermediate language.   Java stored procedures implement the least fixed point functionality for H2. Schemas for facts and rules can be dynamically and transactionally applied to the underlying SQL data store.

Delphinius generates bytecode as the compiled version of a query plan.  The query plan is simply the logic given the inputs to generate the optimal JOOQ result that we can then use for the actual query.  The plan is thus a function ;)  We store these compiled query plans in the database in a metadata schema for Delphinius.