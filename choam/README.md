# CHOAM

## Committee maintenance of State Machine Replication

This module provides a CHOAM abstraction for the group management of a linear distributed log.  This work is loosely based off of ideas from two excellent papers: [From Byzantine Consensus to BFT State Machine Replication: A Latency-Optimal Transformation](https://www.researchgate.net/profile/Alysson_Bessani/publication/254037731_From_Byzantine_Consensus_to_BFT_State_Machine_Replication_A_Latency-Optimal_Transformation/links/562f872108ae4742240af924/From-Byzantine-Consensus-to-BFT-State-Machine-Replication-A-Latency-Optimal-Transformation.pdf) and [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527).

One of the primary goals in Apollo is distributed linear ledgers, composed of database DDL/DML command events.  These are totally ordered transactions on a shared distributed ledger.  This is equivalent to a Kafka event partion, in that this provides an ordered sequence of events - blocks - that all participants process in the same order.  This is, unsuprisingly, the definition of _State Machine Replication_.

In Apollo, however, the model is more complex than simple UTXO transactions.  While some distributed ordering technique may yet emerge (I have hopes) the fact is today, we only really know how to provide total ordering via linear logs. This module provides a committe selection mechanism that is maintained by signed blocks of batches of batches of fundamental transactions.  See [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527) for discussion of the different types of blocks processed by the _CHOAM_.

CHOAM differs from BFT-SMART is that CHOAM is asynchronous and leaderless.  Rather than loosely coupling to a larger consensus as in BFT-SMART, CHOAM is tightly coupled with Ethereal (Aleph-BFT) and CHOAM uses that subsystem to drive consensus behavior using a more primitive unit than blocks.  View changes, checkpoints, and block generation are also leaderless and asynchronous, reusing the same Ethereal consensus model.

## Block production

Ethereal (Aleph) BFT produces pre-blocks _constantly_ .  These pre-blocks may not have any transactions at all.  Consequently, in quiesence where no client transactions are being serviced, these pre-blocks are useless and are consequently discarded.  They do, however, contribute to the counters that drive View reconfiguration, and they certainly contribute to the liveness guarantees of CHOAM.  However, they do not contribute actual state change events, so they do not produce globally visible CHOAM blocks.

## Views

Views go back to the dawn of group communication with ISIS, HORUS and various permutations of such.  A View's membership is defined by taking the View ID.  The ID of a view is simply the previous block hash of the assembly.  By using a block hash, this creates a psuedo random function of the Digest byte width.  In Apollo, this is at minimum 32 bytes, providing an enormous amount of entropy.  Granted, this could theoretically be gamed, but would require a 2/3's majority collaboration, so all right thinking Scottsman will - by design - produce essentially unpredictable block hashes.

## Checkpointing and Bootstrapping

_CHOAM_  provides a model for mitigating one of the more serious issues with distributed ledger technology, that is to say *Bootstrapping*.  CHOAM will periodically emit special _Checkpoint_ blocks that checkpoint the current state
of the chain.  When these checkpoints are processed, the eliminate the usefulness of the blocks before it that went into creating this checkpointed state and may thus be discarded.  What isn't discarded is the View chain of reconfigurations,
all the way back to genesis and it is these blocks that prove the provenance of the Checkpoint and thus the state of the ledger.

To boostrap a node to an existing chain, a special bootstrap process is initiated in the node.  The node waits until a valid block has been produced by the external consensus.  This block is then considered the "anchor" block that the node uses to securely bootstrap against the current group population.  If a checkpoint is available, the node will assemble that checkpoint through gossip with the rest of the membership.  Note that this means that the network load required to obtain the full checkpoint state is distributed across the membership, rather than punishing particular nodes.  Once assembled, the state of the chain is restored, deferred blocks are processed and the node is now ready to participate in the group.

Of course the full chain state can be gathered and moved to cold storage if desired.  Facilities for performing this archiving of the full, entire chain state will be provided in the future to perform this function in a highly available,
exactly once fashion.

## MVP Status

Currently this module is MVP status.  It's now very stable and fast enough to do serious simulations and thus provide the foundation for the next phase of transaction ordering and SQL event generation.  Critical features such as view reconfiguration (periodic, psuedo random committee BFT election), checkpointing, bootstrapping, etc are currently provided.  The system is stable through a very wide range of parameters, and while may perform like crap when configured with such, it will perform correctly.

## CHOAM

The _CHOAM_ is the abstraction a Node (process) uses to coordinate with other members to provide a Consensus driven linear distributed ledger.  The CHOAM utilizes an underlying membership Context and selects committee members based on the ring ordering of a view reconfiguration id.  This gives us a verifiable psudeo random function which is used to determine committee membership.

Periodically, CHOAM will reconfigure the view - i.e. the committee membership.  This is done via a consensus operation between the current view members and the next view members. View keys are generated by the new committee and validated and agreed upon by the current committee.  View keys are deleted after view change (by all right thinking Scottsman) and never reused.  A 2/3 + 1 threshold is required for any CHOAM consensus decision.

The CHOAM provides clients the ability to submit transactions that are then causally ordered and validated by the elected committee.  The committee is composed of collaborators and is leaderless, being driven by the underlying Ethereal (Aleph-BFT) consensus.  Likewise view reconfiguration and genesis bootstrapping are asynchronous and leaderless.

## Finite State Machine Model

CHOAM is driven from a _Finite State Machine_ model using Tron (another module in Apollo).  This manifests in the pattern of a leaf action driver in the form of _contexts_ that provide the leaf actions the Tron state machines.  Currently the FSM model has 4 state maps, reprenting the normal operation of the node (*Merchantile*), block production (*Earner*), the view rotation (*Reconfigure*) and Genesis bootstrapping (*BrickLayer*).  The view reconfiguration logic provides dynamic rotation of view/committee members based on random cuts across the underlying context membership rings on the view context ID (digest).  This balances the load across all available members.

## Messaging

Client transactions are submitted to the current members of the group using Point to Point messaging.  Block production is accomplished with Ethereal Gossip and is reused for view change and genesis bootstrapping consensus as well.  In CHOAM, only a small subset of the membership produces new blocks. Consequently, the other members of the CHOAM must somehow receive these blocks and do so reliably.  The CHOAM group (context) uses the Reliable Broadcast from the _membership_ module to reliably distribute the blocks to all live members using a 2/3+1 variation of the fireflies ring calculation. This protocol's message buffer is bounded and garbage collected and efficient in dissemination. As it is a garbage collected, bounded buffer broadcast, messages will ultimately age out and discared. The protocol is thus ultimately partially synchronous. Members can thus fall out of (partial) synchronization of the group. Currently this is handled by resynchronizing with the available group through the same bootstrap mechanism a node uses to join anew.
