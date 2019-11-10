# Apollo Avalanche

Based on the most excellent paper [Scalable and Probabilistic Leaderless BFT Consensus through Metastability](https://arxiv.org/abs/1906.08936).

Apollo Avalanche is built on top of the Fireflies communications layer.

## Status

* No integration of smart contracts or transaction validation
    * this means that there is no conflict set determination, and thus all transactions are "virtuous"
* Parent selection for transactions is simplistic
* Noop transactions generation is also simplistic
* Uses TLS connections, rather than MTLS connections
    * I believe this is the correct route, as the nature of the Avalanche protocol is gossip based and consequently requires randomized communication with all nodes in the system


## Implementation Notes

* _Apollo Avalanche was designed to work with the underlying Fireflies communication subsystem_ - consequently there are interesting implementation details that dovetail into the Fireflies base.  


* _Member sampling driven off of Fireflies rings_ - rather than creating an alternative mechanism to randomly sample members for Avalanche queries.  The current state of the system leverages the randomized graph created by the Fireflies membership rings to provide a random sampling of the underlying membership without resorting to costly set iteration and provides a memory, so no unintended duplication occurs on subsequent queries.

* _DAG working set management_ - Apollo manages the set of unfinalized transactions in memory, using a simple, custom graph model: The **WorkingSet**.  The working set is composed of unfinalized DAG nodes.  The WorkingSet provides the DAG abstraction and operation API used by Apollo's Avalanche implementation.


* _MapDB for finalized txn storage_ - [MapDB](https://jankotek.gitbooks.io/mapdb/content/) provides the off-heap cache and persistent storage for finalized User (i.e. not NoOp) DAG nodes.  This drastically simplifies issues with Java garbage collection, with the structure being maintained in non Java heap memory.  The memory mapped BTree map provides the final, persistent storage for finalized DAG nodes, indexed by the node's hash.  The persistent DAG storage is fed by an in memory (but off java heap) cache of newly finalized transactions.  This cache provides a tradeoff of speed vs. durability.


## Perf Notes

Currently, no real work has done on performance for Apollo.  In current toy perf testing, we're seeing about 1500 TPS with about 38% of a single CPU per simulated node, without network (MTLS/TLS) and connection overhead.  Currently, the perf testing is rate limited on input processing.  There's a decent amount of crap Apollo is tracking, and there's locks, thus there's a decent amount of concern in the Apollo design around DAG working storage and access.

About 25% of the time in Apollo Avalanche is spent in graph traversal during the isStronglyPreferred quey operation.  The determination is required many times by other queries from other members, so the isStronglyPreferred determination is cached in the WorkingSet.  Time spent in SHA256 hashing (there's a lot of it in Apollo) is optimized by using ThreadLocal instances of the SHA256 hash alogorithm.  About 3% of the time is spent in deserializing the DagEntries via Avro.  Finalization and other operations collectively use < 10%.  The rest of the time in perf testing is spent waiting with thread sleep.

Generating finalized transactions at > 1500 TPS generates **a lot** of DAG nodes.  This can easily outgrow existing disk space.  MapDB has a very high upper limit on the cardinality it can store, but billions is still billions and so I'll eventually have to provide a rolling cold storage capability for Apollo (as well as checkpointing and other common DB log tricks, hopefully).

## Design Notes

Apollo Avalanche has five major subsystems:
  * Node submission - birth of user txns
  * Avalanche outbound query
  * Avalanche inbound query
  * DAG node preference
  * DAG node finalization

DAG nodes are created at a given process and inserted into that process' WorkingSet.  The newly birthed transaction is primarily replicated across other Apollo nodes via the Avalanche query.  An auxillary gossip is also used to request unknown DAG nodes from other members.  The outbound Avalanche query pulls a batch of unqueried DAG nodes, and chooses K random members from the group (the Fireflies rings are used as a random membership sample generator) and queries these members for the preference of the node batch (batch size configurable, currently 40).  In the processing of the inbound query, the Apollo node first inserts any unknown DAG nodes from the query batch into the WorkingSet before the _isStronglyPreferred_ query is executed against the working set.


So the optimization space is complicated.  Apollo leverages the "not yet finalized" state of the DAG store in the working set.  The unfinalized set of nodes is a graph, of course.  Apollo optimizes this unfinalized graph by removing any links referencing finalized nodes.  Apollo caches the result of isStronglyPreferred querying and maintains the cached state through cache invalidation if the node, or a parent of the node changes its isStronglyPreferred state - via preference change in the Conflict Set of the node, or through the filling in of previoously _unknown_ DAG nodes.

When a DAG node is finalized, the closure of this node is also finalized. The WorkingSet performs a delete of the finalized node, eliminating the transitive closure of the node from all unfinalized nodes, cleaning up the closure.  This means less and less work as parents are finalized, conviently progressing the Avalanche protocol because that's N less nodes to check up on for "isStronglyPreferred", preference and finalization (geesh).  So, working out well.

Unfinalized DAG nodes in the working set keep track of not only their direct parents, but also their direct dependents - i.e. DAG nodes that point to the node as a parent.  This forms a doubly linked DAG structure.  This structure optimizes the common cases of removing finalized nodes and their transitive closure as well as replacing "holes" in the working set as previously unknown - and therefore dangling - links are discovered.  The working set represents unknown (dangling) links with the aptly named UnknownNode.  As mentioned previously, an auxillary gossip supplements the Avalanche query gossip by trying to fill in known unknown nodes in the DAG from other members within the group. 

## Reuse Of Apollo Fireflies

A key premise of Apollo is the aggressive reuse of the underlying Fireflies substrate.  FF provides a byzantine tolerant 2*T+1 partitioning of the underlying membership graph.  This provides a sampling space of randomized member subsets Apollo uses for Avalanche querying.  We are currently working on validator partitioning that reuses Firefly rings, as another example of the reuse of the underlying FF substrate.  This means Apollo is leveraging what amounts to a verifiable random function of low cardinality across the membership set.  With byzantine tolerance, assuming the underlying FF substrate is parameterized correctly.

## DagViz

Given that the structure of the Avalanche DAG is central to understanding the protocol and behavior of Apollo, a simple generator for visualizing the DAG is provided by the _DagViz_ class.  This utility will dump the DAG in DOT format, or generate DOT images.  Be warned, however.  DOT, Gephi and other graph visualization tools cannot handle huge numbers of nodes, and at 1.5K TPS, it doesn't take much to create a huge number of nodes.

