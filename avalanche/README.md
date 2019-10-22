# Apollo Avalanche

Based on the most excellent paper [Scalable and Probabilistic Leaderless BFT Consensus through Metastability](https://arxiv.org/abs/1906.08936).

Apollo Avalanche is built on top of the Fireflies communications layer and this implementation leverages Fireflies in important ways.  Consequently, understanding the Fireflies layer is key to understanding the Apollo Avalanche implementation.

## Status

* No integration of smart contracts or transaction validation
    * this means that there is no conflict set determination, and thus all transactions are "virtuous"
* Parent selection for transactions is simplistic
* Noop transactions generation is also simplistic
* Uses TLS connections, rather than MTLS connections
    * I believe this is the correct route, as the nature of the Avalanche protocol is gossip based and consequently requires randomized communication with all nodes in the system


## Implementation Notes

* _Apollo Avalanche was designed to work with the underlying Fireflies communication subsystem_ - consequently there are interesting implementation details that dovetail into the Fireflies base.


* _Avalanche rounds driven off of the underlying Fireflies gossip rounds_ - This vastly simplifies scheduing and integrates Avalanche with the Fireflies communication overlay guarantees.  


* _Member sampling driven off of Fireflies rings_ - rather than creating an alternative mechanism to randomly sample members for Avalanche queries.  The current state of the system leverages the randomized graph created by the Fireflies membership rings to provide a random sampling of the underlying membership without resorting to costly set iteration and provides a memory, so no unintended duplication occurs on subsequent queries.


* _Use Fireflies Reliable Broadcast for Transaction flooding_ - this is deviation from the original Avalanche protocol.  Rather than using the Avalanche queries to propagate the transactions across the member nodes, Apollo Avalanche uses the underlying Reliable Broadcast facilities of of the Fireflies layer for flooding the transactions across the network.  This, imo, is far more efficient and reliable, and it's worth noting that the Avalanche paper specifically notes that an supplemental gossip flood is used in their implementation.


* _H2 SQL Database for DAG storage_ - [H2](https://www.h2database.com/html/main.html) is a very fast in process SQL database that Apollo uses for DAG ledger state storage.  There's more optimization work needed on my implementation, but it works extremely well and may be run in memory or file based.


## Perf Notes

Currently, no real work has done on performance for Apollo.  In current toy perf testing, we're seeing about 140 TPS with about 38% of a single CPU, without network (MTLS/TLS) and connection overhead.  This is with a small (13 member) cardinality group, but is conviently the 2*T+1 failure group cardinality configured for the 100 member (pregen'd test group).  Currently, the perf testing is rate limited on input processing.  There's a decent amount of crap Apollo is tracking, and there's locks, thus there's a decent amount of concern in the Apollo design around database schema and access.

The current input throughput in a node is around 11 TPS.  So, the max throughput of the toy test is 13 * 11.  So Apollo can at least process queries and prefer/finalize at about the input rate we can sustain.  Note that the effective input rate would scale with the number of nodes in the toy test.  So doubling the number of clients (assuming CPU to do so) should double the TPS.  Of course, non linear effects with the parameterization optimization for a small group comes into play and...  Well.  That's left for the time I have scheduled for THE FUTURE for doing moar serious perf.

The input rate for a given node is gated (from perf and metrics) by the selection of parents for the transaction.  Currently Apollo amortizes this across multiple inputs, using a deque of the last randomized query.  Likewise with the no op txn generation.  Both of which appear to work well.

## Design Notes

Apollo Avalanche is basically a set of independent threads managing a pipeline.  Logically, we have the first input - i.e. the BIRTH - of a txn at a given node.  This requires a connection pool and is potentially expensive because of parent frontier selection.  Then we put the hashed and parented txn in the input queue where Apollo batches those up per txn.  From there the query process takes over, getting a batch of txns that have not been queried and doing the Avalanche query.  Then txns get put into the preference queue, and then finally (haha) the finalization queue.  Each part of the pipeline has its own thread.  Inbound queries are parallel limited by the size of the queryPool JDBC pool and the inbound throughput capacity of Netty (in the toy, it's infinite, of course).

So the optimization space is complicated.  Currently, H2 performs pretty good, although Apollo is currently only using in-memory databases, so switching to persistent file based Avalanche DAG storage will take additional optimizing.  With more available txns, parent selection from the frontier has more to choose from and thus the amortization of "frontierSample" starts to pay off.  Currently, the big plug is birth input and parent selection, which constitutes about 30% of the toy per testing time.  Query, preference and finalization batches are easily accommidated in the toy, so there's room  to expand.

The SQL model of the DAG storage in H2 has turned out very well.  The optimizations are all SQL optimization tricks and better queries.  Stuff that has - e.g. - Stack Overlow answers ;).  The DAG storage in Apollo is basically a DAO, so the implementation can be swapped out easily.  First, by simply using a different JDBC URL (although the dialect might have to be tweaked) and next by just reimplementing the DAO.  When this stabilizes, the Dag will become an SPI interface for the store plugin.  But for now, Apollo can use in memory or file based with just the config of the JDBC url, so not a burning issue at the moment.

## SQL Schema Notes

The current Dag Schema for Apollo Avalanche is a traditional star schema.  The central table is the DAG table and everything else pretty much revolves around this table.  During early perf testing, it became clear that the contention between the logical pipelines in the Avalanche protocol required some interesting SQL structure.  Transactions are split into two types: User Txns and NoOp transactions.  The latter is required to create votes for neglected transactions (see the original paper for details).  User transactions never parent NoOp transactions.  And NoOp transactions never parent other NoOp transactions (by rights, all Right Thinking Scottsman).

Apollo leverages the "not yet finalized" state of the DAG store.  In the Apollo DAG store, unfinalized transactions contain a closure of the unfinalized parents of the transaction.  This is commonly known as the DAG Closure model for representing DAGs in SQL.  It's been an efficient model for Apollo because it is a garbage collected set that is _no longer useful after the transaction has been finalized_.  This means that, while _technically_ the SQL DAG Closure model is N^2 in storage costs, in __practice__ the closure of any given unfinalized DAG node is bounded by the DAG closure of the node's parents, not including parents that have been finalized.

When a DAG node is finalized, the closure of this node is also finalized.  So a frickin' expensive mass delete is performed by Apollo, eliminating the transitive closure of a txn from all txn nodes still outstanding.  Conviently progressing the Avalanche protocol because that's N less nodes to check up on for "isStronglyPreferred", preference and finalization (geesh).  So, working out well.
