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


* _H2 SQL Database for DAG storage_ - H2 is a very fast in process SQL database that Apollo uses for DAG ledger state storage.  There's more optimization work needed on my implementation, but it works extremely well and can be run in memory or file based.
