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

* _Randomized, Unique Sample From Membership View_ - Avalanche is driven off of a _presumed_ radom ordering of the group membership.  This is represented by the _ViewSampler_.


* _Member sampling driven off of Fireflies view_ - Member sampling is currently driven off of a brain dead randomized sample mechanism which can presumably scale to 5 or 6 orders of magnitude in membership cardinality.  This is driven off the fireflies live view membership, but I intend to abstract this so any membership supplier can provide the random sampling.

* _DAG working set management_ - Apollo manages the set of unfinalized transactions in memory, using a simple, custom graph model: The **WorkingSet**.  The working set is composed of unfinalized DAG nodes.  The WorkingSet provides the DAG abstraction and operation API used by Apollo's Avalanche implementation.


* _MapDB for finalized txn storage_ - [MapDB](https://jankotek.gitbooks.io/mapdb/content/) provides the off-heap cache and persistent storage for finalized User (i.e. not NoOp) DAG nodes.  This drastically simplifies issues with Java garbage collection, with the structure being maintained in non Java heap memory.  The memory mapped BTree map provides the final, persistent storage for finalized DAG nodes, indexed by the node's hash.  The persistent DAG storage is fed by an in memory (but off java heap) cache of newly finalized transactions.  This cache provides a tradeoff of speed vs. durability.


## Perf Notes

A modest amount of work has done on performance for Apollo - 100 nodes in a single EKS cluster.  In current local perf testing, we're seeing about 2500 TPS with about 38% of a single CPU per simulated node, without network (MTLS/TLS) and connection overhead.  The perf seems - ultimately - driven off of number of txns queried per second.  The MTLS version of this is batch oriented, and uses 40 txns per batch.

About 25% of the time in Apollo Avalanche is spent in graph traversal during the isStronglyPreferred quey operation.  The determination is required many times by other queries from other members, so the isStronglyPreferred determination is cached in the WorkingSet.  Time spent in SHA256 hashing (there's a lot of it in Apollo) is optimized by using ThreadLocal instances of the SHA256 hash alogorithm.  About 3% of the time is spent in deserializing the DagEntries via Avro.  Finalization and other operations collectively use < 10%.  The rest of the time in perf testing is spent waiting with thread sleep.

Generating finalized transactions at > 2500 TPS generates **a lot** of DAG nodes over any reasonable time period.  This is no longer limited by the MapDB disk based implementation, as the current build delegates this to a listener for cold storage of ye entire DAG tree (finalized!) - a listener which is EMPTY and therefore the fastest one available!  Apollo uses [MapDB](http://www.mapdb.org/) to provide an off Java heap) cache of the last N (parameterized) finalized user transactions (no NoOp).  Spilling this to disk - even through mem mapped BTrees - is teh suck, so it is expected that batch optimized linear, index-less cold storage is mandatory for transcription of ye DAG.  And even then, relegated to a trusted continous availability group elected from the membership as redundant cold storage dumpers.

Word.

## Design Notes

Apollo Avalanche has five major subsystems:
  * Node submission - birth of user txns
  * Avalanche outbound query
  * Avalanche inbound query
  * DAG node preference
  * DAG node finalization
  * DAG Alt Gossip

DAG nodes are created at a given process and inserted into that process' WorkingSet.  The newly birthed transaction is primarily replicated across other Apollo nodes via the Avalanche query.  An _auxillary gossip_ is also used to request unknown DAG nodes from other members, currently piggybacked on a randomly chosen member query.  The outbound Avalanche query pulls a batch of unqueried DAG nodes, and chooses K random members from the group (the Fireflies rings are used as a random membership sample generator) and queries these members for the preference of the node batch (batch size configurable, currently 40).  In the processing of the inbound query, the Apollo node first inserts any unknown DAG nodes from the query batch into the WorkingSet before the _isStronglyPreferred_ query is executed against the working set.


So the optimization space is complicated.  Apollo leverages the "not yet finalized" state of the DAG store in the working set.  The unfinalized set of nodes is a graph, of course.  Apollo optimizes this unfinalized graph by removing any links referencing finalized nodes.  Apollo caches the result of isStronglyPreferred querying and maintains the cached state through cache invalidation if the node, or a parent of the node changes its isStronglyPreferred state - via preference change in the Conflict Set of the node, or through the filling in of previoously _unknown_ DAG nodes.

When a DAG node is finalized, the closure of this node is also finalized. The WorkingSet performs a delete of the finalized node, eliminating the transitive closure of the node from all unfinalized nodes, cleaning up the closure.  This means less and less work as parents are finalized, conviently progressing the Avalanche protocol because that's N less nodes to check up on for "isStronglyPreferred", preference and finalization (geesh).  So, working out well.

Unfinalized DAG nodes in the working set keep track of not only their direct parents, but also their direct dependents - i.e. DAG nodes that point to the node as a parent.  This forms a doubly linked DAG structure.  This structure optimizes the common cases of removing finalized nodes and their transitive closure as well as replacing "holes" in the working set as previously unknown - and therefore dangling - links are discovered.  The working set represents unknown (dangling) links with the aptly named UnknownNode.

As mentioned previously, an auxillary gossip supplements the Avalanche query gossip by trying to fill in known unknown nodes in the DAG from other members within the group. Currently, out of K members sampled for the Avalanche query, one will be chosen to query the unknown DAG nodes the Apollo node is currently tracking in its working set. Parameterizing this vs the number of simultaneous queries is teh suck, so not even tried.  The lucky chosen member - 1 out of K queried members - gets the "wanted" list from the node's _WorkingSet_.  The lucky node just returns - in addition to the query response - the txns "wanted" by the querying node.  Seems to work pretty well with low overhead (i.e. 1/K).

## Reuse Of Apollo Fireflies

A premise of Apollo is the aggressive reuse of the underlying Fireflies substrate.  FF provides a byzantine tolerant 2xF+1 partitioning of the underlying membership graph. We are currently working on validator partitioning that reuses Firefly rings, as another example of the reuse of the underlying FF substrate.  This means Apollo is leveraging what amounts to a verifiable random function of low cardinality across the membership set.  With byzantine tolerance, assuming the underlying FF substrate is parameterized correctly.  Using a linear chain of txns (i.e. DAG nodes) we can use the previous finalized "state" hash and map that into each of the underlying fireflies rings.  This gives us 2xF+1 members that form a BFT subset which can be used for group elections, which can be determined locally by each node.

The interaction between local - i.e. relativistic - finalization and global - i.e. Kairos time - is... interesting.  So validation of previously - i.e. globally - finalized txns in bootstraping or catch up is rather interesting at the moment.  Currently it is theorized that Kairos transactions, being gossiped or _satisfied_ represent hole filling in the ontological causal closure of the current Apollo node's _WorkingSet_.  Much currently remains to be explored and delineated.

## DagViz

Given that the structure of the Avalanche DAG is central to understanding the protocol and behavior of Apollo, a simple generator for visualizing the DAG is provided by the _DagViz_ class.  This utility will dump the DAG in DOT format, or generate DOT images.  Be warned, however.  DOT, Gephi and other graph visualization tools cannot handle huge numbers of nodes, and at 1.5K TPS, it doesn't take much to create a huge number of nodes.

