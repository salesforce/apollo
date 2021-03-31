#Apollo Replicated SQL State Machine
The  _sql-state_  module provides a high fidelity SQL Materialized View from a linear log.  The model provided by this module is a SQL database.
This SQL database has a single writer, which is the linear log fed into the _SqlStateMachine_.
The log is a sequence of transactions to execute against the current SQL state of the database.  As these transactions can contain SQL Data Definition Language statements,
this also means that the manipulation of the meta data of the SQL database is also part of the log.  This allows this system to provide a full featured
SQL database that represents the "current state" of the linear log.  This is, in essence, a Materialized View. Because this is a full featured SQL database,
this model include stored procedures, triggers, functions, indexes as well as schemas, tables, types, and even views (a view with a view ;) ).


## Model
The input to the SQL state machine is a linear log.  This log is composed of Blocks of transactions, which are submitted, in order, to the state machine.
The transactionis then executed agains the local database representing the materialized view of this state.  Results can be returned from these transactions,
such as the _Call_ results from a SQL function.  Arguments may be supplied as well, which essentially means that each of the executed transactions by this module
represent a kind of anonymous stored procedure executed at the "server" - in this case, the in process H2 Database that represents the materialized view.

### Transaction Execution
Transaction exeution is performed via the single JDBC connection to the underlying H2 database.  This implies that the contents of the transactions are representable
with H2 SQL.  Sadly, "generic" SQL is not really a thing, but a convention, and so this is not a generic SQL execution.  Further, the transaction execution model
is JDBC, which constrains the styles of interaction as well as the argument value and return types.
### Transaction Types
Transactions are one of the following types, and are represented as Protobuffs defined in the _cdc.proto_ file.
* Statement
* Call
* Batch
* BatchUpdate
* Script

#### Statement
The _Statement_ is the equivalent of the JDBC SQL statement with or without arguments.  Return value
#### Call
The _Call_ is the transaction to execute SQL stored procedures.