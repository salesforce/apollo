# Consortium

## Committee maintenance of State Machine Replication

This module provides a _Consortium_ abstraction for the group management of a linear distributed log.  This work is based off of two excellent papers: [From Byzantine Consensus to BFT State Machine Replication: A Latency-Optimal Transformation](https://www.researchgate.net/profile/Alysson_Bessani/publication/254037731_From_Byzantine_Consensus_to_BFT_State_Machine_Replication_A_Latency-Optimal_Transformation/links/562f872108ae4742240af924/From-Byzantine-Consensus-to-BFT-State-Machine-Replication-A-Latency-Optimal-Transformation.pdf) and [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527).

One of the primary goals in Apollo is distributed linear ledgers, composed of database change data capture events.  These are totally ordered transactions on a shared distributed ledger.  This is equivalent to a Kafka event partion,
in that this provides an ordered sequence of events - blocks - that all participants process in the same order.  This is, unsuprisingly, the definition of _State Machine Replication_.

In Apollo, however, the model is more complex than simple UTXO transactions.  While some distributed ordering technique may yet emerge (I have hopes) the fact is today, we only really know how to provide total ordering via linear logs.
This module provides a committe selection mechanism that is maintained by signed blocks of batches of batches of fundamental transactions.  See [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527) for discussion of the different types of blocks processed by the _Consortium_.

## Checkpointing and Bootstrapping

_Consortium_  provides a model for mitigating one of the more serious issues with distributed ledger technology, that is to say *Bootstrapping*.  Consortium will periodically emit special _Checkpoint_ blocks that checkpoint the current state
of the chain.  When these checkpoints are processed, the eliminate the usefulness of the blocks before it that went into creating this checkpointed state and may thus be discarded.  What isn't discarded is the View chain of reconfigurations,
all the way back to genesis and it is these blocks that prove the provenance of the Checkpoint and thus the state of the ledger.

To boostrap a node to an existing chain, a special bootstrap process is initiated in the node.  The node waits until a valid block has been produced by the external consensus.  This block is then considered the "anchor" block that the node
uses to bootstrap against the current group population.  If a checkpoint is available, the node will assemble that checkpoint through gossip with the rest of the membership.  Note that this means that the network load required to obtain the
full checkpoint state is distributed across the membership, rather than punishing particular nodes.  Once assembled, the state of the chain is restored, deferred blocks are processed and the node is now ready to participate in the group.

Of course the full chain state can be gathered and moved to cold storage if desired.  Facilities for performing this archiving of the full, entire chain state will be provided in the future to perform this function in a highly available,
exactly once fashion.

## MVP Status

Currently this module is MVP status.  It's stable and fast enough to do serious simulations and thus provide the foundation for the next phase of transaction ordering and CDC event generation.  However, critical features such as
view reconfiguration, checkpointing, bootstrapping, etc are not currently provided.  There are also known issues with parameters that can cause endless view changes because the system cannot process transactions fast enough for the
timeout parameters.  It's hoped this can be eliminated with feedback generated from state context actions, but perhaps it can simply be bounded with "good" parameter checks.  SMH ;)

## Consortium

The _Consortium_ is the abstraction a Node (process) uses to coordinate with other members to provide a Consensus driven linear distributed ledger.  The Consortium utilizes an underlying membership Context and selects committee
members based on the ring ordering of a context view's id.  This gives us a verifiable psudeo random function which is used to determine committee membership.

The Consortium provides clients the ability to submit transactions that are then ordered and validated by the elected committee.  The committee is composed of collaborators, and leadership is managed via the mechanisms outlined in [From Byzantine Consensus to BFT State Machine Replication: A Latency-Optimal Transformation](https://www.researchgate.net/profile/Alysson_Bessani/publication/254037731_From_Byzantine_Consensus_to_BFT_State_Machine_Replication_A_Latency-Optimal_Transformation/links/562f872108ae4742240af924/From-Byzantine-Consensus-to-BFT-State-Machine-Replication-A-Latency-Optimal-Transformation.pdf).  This provides a leadership based block generation with dynamic view membership.

## Finite State Machine Model

Consortium is driven from a _Finite State Machine_ model using Tron (another module in Apollo).  This manifests in the pattern of a leaf action driver in the form of the _CollaboratorContext_.  This class is used as the _Context_
of the Tron state machines.  Currently the FSM model has 3 state maps, reprenting the normal operation of the node, regency change (leadership change) and the generation of a view (liveness, Genesis generation, and dynamic rotation
of view members).

## Messaging

Consortium uses a mixture of Broadcast as well as point to point messaging.  Client transactions are submitted to the current members of the group using Point to Point messaging.  Likewise the joing of a group by a new/formerly inactive member is also point 2 point.  Block generation, validation is broadcast to ensure replication across the group in a byzantine tolerant fashion.

## Pluggable Consensus

Consortium is designed to use another, group wide consensus mechanism to totally order blocks within the underlying Consortium Context.  Consortium also relies upon this underlying consensus to replicate the blocks throughout the 
membership.  The committee is used simply to distribute processing load - transaction simulation and result production - fairly and randomly throughout the
membership (selection process may be policy weighted, 'natch).  Block generation is leader based and generated blocks, once validated, are submitted to the supplied consensus.  In Apollo, this is currently tested with the Avalanche
consensus, but will likely be moved to a Snowball protocol or similar, due to the linear nature of the log (i.e. not DAG based).
