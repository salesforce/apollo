# Apollo Replicated SQL State Machine

The  _sql-state_  module provides a high-fidelity SQL Materialized View from a linear log. The model provided by this
module is a SQL database.
This SQL database has a single writer, which is the linear log fed into the SqlStateMachine.
The log is a sequence of transactions to execute against the current SQL state of the database. As these transactions
can contain SQL Data Definition Language statements,
this also means that the manipulation of the meta-data of the SQL database is also part of the log. This allows this
system to provide a full-featured
SQL database that represents the "current state" of the linear log. This is, in essence, a Materialized View. Because
this is a full-featured SQL database,
this model includes stored procedures, triggers, functions, indexes as well as schemas, tables, types, and even views (a
view with a view).

## Model

The input to the SQL state machine is a linear log of SQL commands. This log is composed of Blocks of transactions,
which are submitted in order to the state machine.
Each transaction is then executed against the local database that represents the materialized view of this state.
Results may be returned from these transactions,
such as the _Call_ results from a SQL function. Arguments may be supplied as well, which essentially means that each of
the executed transactions by this module
represents a kind of anonymous stored procedure executed at the "server" - in this case, the in embedded H2 Database for
this CHOAM log that represents its materialized view.

### Transaction Execution

Transaction exeution is performed via the single JDBC connection to the underlying H2 database. This implies that the
contents of the transactions are representable
with H2 SQL. Sadly, "generic" SQL is not really a thing, but a convention, and so this is not a  _generic_  SQL
execution. Further, the transaction execution model
is JDBC, which constrains the styles of interaction as well as the argument value and return types.

When a transaction is submitted, the client submitting the transaction can provide a function to execute when the
transaction is finalized. This function takes
the value returned — possibly null - and the error raised - if any. This means that calls, scripts, prepared statements,
etc, and return values in addition to
the normal SQL execution, providing a very powerful mechanism for transactions against an SQL store that we normally
take
for granted.

### Database Compatibility

Note that the H2 database has the ability to  _emulate_  several popular databases (it's one of
its [fine selling points](http://www.h2database.com/html/features.html#compatibility)). Currently, this is only
available on the JDBC connection creation to the H2 embedded instance and thus is not currently supported by Apollo.
This will be accomidated in the future.

### Schema Evolution

The SQL state machine supports the full gamut of the SQL Data Definition Language (DDL). Note, however, that for H2 and
thus the SQL state machine, DDL is not transactional, and the current transaction will be committed on each DDL
statement. The DDL statements cannot be rolled back. Thus, one can wedge one's self quite easily with ill-advised schema
evolution strategies. In the interests of providing as many sharp knives as needed, there are currently no restrictions
on DDL.

Schema may also be maintained by executing [Liquibase](https://docs.liquibase.com/home.html) Migration transactions.
This transaction type includes a Liquibase command and change log that is applied to the H2 SQL store. Note that the
actual change log is not stored in the DB, rather it exists only in the CHOAM log.

### Transaction Types

Transactions are one of the following types, and are represented as Protobuffs defined in the sql-state.proto file.

* Statement
* Call
* Batch
* BatchUpdate
* Script
* BatchedTransaction
* Migration

#### Statement

The Statement is the equivalent of the JDBC SQL prepared statement with or without arguments.

#### Call

The Call is the transaction to execute SQL stored procedure Calls.

#### Batch

A Batch of SQL statements with no arguments, executed in order

#### BatchUpdate

A Single prepared statement that is executed in batch with a list of arguments, one argument set per batch entry

#### Script

A Java function that accepts an SQL connection and may return results — basically an anonymous function

#### BatchedTransaction

This is a batch of any of the types (even recursively), executed in order in a single transaction

#### Migration

A Liquibase Database migration

## Stored Procedures, Functions, and Triggers

The model provides the definition and execution of user-defined SQL stored procedures, functions, and triggers. These
are
currently limited to Java implementations, although more languages and WASM support is
straightforward to add. Java was chosen as the first implementation because - frankly - it's a shitload more mature
than all the others. Not simply from
a "been around longer" but from a "has standard interfaces for things like SQL." Seriously, it's hard to come up with
these things, and a "bring your own SQL connection library" does not
lend itself well to the issues of SQL state machine replication.

## Deterministic Implementation

Due to the model used for SQL state, it's essential that every node executes the transactions with identical results.
That's how we achieve replicated state across the system.
However, things like TIME and RANDOM make that impossible. So, the underlying H2 database used by this module has been
modified to provide for deterministic SQL execution. Even though
we provide the RANDOM function, we guarantee that the results of these RANDOM function invocations will be identical
across all nodes. Likewise, with TIME and some other functions. This
is accomplished by slight modifications of the underlying H2 database, and the use of block hashes for seeding these
functions. This results in deterministic SQL execution across the system.

Likewise, it's important in the Java stored procedures, functions and triggers to be deterministically executed. This is
not
enforced at the moment.

## Checkpointing and Bootstrapping

Checkpoints are implemented with H2's _SCRIPT_ command, which dumps the current database state in a form that will
recreate the state of the database. This is compressed and become the checkpoint state
used in CHOAM for bootstrapping nodes. Currently, no facilities are implemented for incremental backup, but it should be
straightforward to implement an incremental scheme. For the future ;)

