# CHOAM

## Committee maintenance of State Machine Replication

This module provides a CHOAM abstraction for the group management of a linear distributed log. This work is loosely
based off of ideas from two excellent
papers: [From Byzantine Consensus to BFT State Machine Replication: A Latency-Optimal Transformation](https://www.researchgate.net/profile/Alysson_Bessani/publication/254037731_From_Byzantine_Consensus_to_BFT_State_Machine_Replication_A_Latency-Optimal_Transformation/links/562f872108ae4742240af924/From-Byzantine-Consensus-to-BFT-State-Machine-Replication-A-Latency-Optimal-Transformation.pdf)
and [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527).

One of the primary goals in Apollo is distributed linear ledgers, composed of database DDL/DML command events. These are
totally ordered transactions on a shared distributed ledger. This is equivalent to a Kafka event partition, in that this
provides an ordered sequence of events - blocks - that all participants process in the same order. This is,
unsuprisingly, the definition of _State Machine Replication_.

In Apollo, however, the model is more complex than simple UTXO transactions. This module provides a committee selection
mechanism that is maintained by signed blocks of batches of fundamental
transactions.
See [From Byzantine Replication to Blockchain: Consensus is only the Beginning](https://arxiv.org/abs/2004.14527) for
discussion of the different types of blocks processed by the _CHOAM_.

CHOAM differs from BFT-SMART is that CHOAM is asynchronous and leaderless. Rather than loosely coupling to a larger
consensus as in BFT-SMART, CHOAM is tightly coupled with Ethereal (Aleph-BFT) and CHOAM uses that subsystem to drive
consensus behavior using a more primitive unit than blocks. View changes, checkpoints, and block generation are also
leaderless and asynchronous, reusing the same Ethereal consensus model.

## Block production

Ethereal (Aleph) BFT produces pre-blocks _constantly_ . These pre-blocks may not have any transactions at all.
Consequently, in quiescence, where no client transactions are being serviced, these pre-blocks are useless and are
consequently discarded. They do, however, contribute to the counters that drive View reconfiguration, and they certainly
contribute to the liveness guarantees of CHOAM. However, they do not contribute to actual state change events, so they
do
not produce globally visible CHOAM blocks.

## Views

Views go back to the dawn of group communication with ISIS, HORUS, and various permutations of such. A View's membership
is defined by taking the View ID and using this to obtain the successors of that id - digest - across the view's Context
Fireflies
rings. The ID of a view is the XOR of:

* the hash of the previous assembly block
* the consensus View HexBloom diadem determined for that assembly

This creates a psuedo random function of the Digest algorithm byte width. In Apollo, this is at minimum
32 bytes, providing an enormous amount of entropy. Granted, this could theoretically be gamed, but would require a 2/3's
majority
collaboration, so all right-thinking Scotsman will - by design - produce essentially unpredictable block hashes.

## Asynchronous Joins

Periodically, CHOAM will change the committee. These committees are selected by the View IDs, as previously described.
The protocol is

1. Determine consensus View
2. Produce Assemble block with consensus view diadem
3. Start block production for current view
4. Await at least a majority of the next View's members to propose their view keys
5. Emit Reconfiguration block and finish

The block production for each view is blocked until View consensus happens within the current Committee. Once a View has
been selected, the Assemble block provides the signal to the joining members of the next committee to start the Join
process.
CHOAM uses ephemeral keys for each member per view and the Join protocol is the process of collecting these keys from
the next View's members and recording these keys in the corresponding Reconfiguration block produced at the end of the
processing for this view. The emitting of the Reconfiguration block is the end of the current view. The processing
of the Reconfiguration block is the beginning of the processing of the next View.

## Checkpointing and Bootstrapping

_CHOAM_  provides a model for mitigating one of the more serious issues with distributed ledger technology,
*Bootstrapping*.
In the production of work, CHOAM will periodically emit special _Checkpoint_ blocks that will checkpoint the current
state
of the chain. When these checkpoints are processed, they eliminate the usefulness of the previous blocks before it that
went into
creating this checkpoint state and these blocks can be discarded. CHOAM keeps the View chain of reconfigurations
all the way back to genesis, and it is this chain of reconfiguration blocks that prove the provenance of the Checkpoint
and thus the state of the system maintained by the ledger.

To boostrap a node to an existing chain, the node first joins the common broadcast group and a special bootstrap process
is initiated. The node waits until a valid block has been produced by the external consensus. This block is then
considered the "anchor" block that the node uses to securely bootstrap against the current group population. If a
checkpoint is available, the node will assemble that checkpoint through gossip with the rest of the membership.

Note that this means that the network load required to obtain the full checkpoint state is distributed across the
membership, rather than punishing particular nodes. Once assembled, the state of the chain is restored, deferred
blocks are processed, and the node is now ready to participate in the group.

Of course, the full chain state can be gathered and moved to cold storage if desired. Facilities for performing this
archiving of the full, entire chain state will be provided in the future to perform this function in a highly available,
exactly once fashion.

## MVP Status

Currently, this module is MVP status. It's now very stable and fast enough to do serious simulations and thus provide
the
foundation for the next phase of transaction ordering and SQL event generation. Critical features such as view
reconfiguration (periodic, psuedo random committee BFT election), checkpointing, bootstrapping, etc. are currently
provided. The system is stable through a very wide range of parameters, and while it may perform like crap when
configured
with such, it will perform correctly.

## CHOAM

The _CHOAM_ is the abstraction a Node (process) uses to coordinate with other members to provide a Consensus-driven
linear distributed ledger. The CHOAM utilizes an underlying membership Context and selects committee members based on
the ring ordering of a view reconfiguration id. This gives us a verifiable psudeo random function which is used to
determine committee membership.

Periodically, CHOAM will reconfigure the view — i.e., the committee membership. This is done via a consensus operation
between the current view members and the next view members. View keys are generated by the new committee and validated
and agreed upon by the current committee. View keys are deleted after view change (by all right-thinking Scotsman) and
never reused. A 2/3 + 1 threshold is required for any CHOAM consensus decision.

The CHOAM provides clients the ability to submit transactions that are then causally ordered and validated by the
elected committee. The committee is composed of collaborators and is leaderless, being driven by the underlying
Ethereal (Aleph-BFT) consensus. Likewise, view reconfiguration and genesis bootstrapping are asynchronous and
leaderless.

## Finite State Machine Model

CHOAM is driven from a _Finite State Machine_ model using Tron (another module in Apollo). This manifests in the pattern
of a leaf action driver in the form of _contexts_ that provide the leaf actions the Tron state machines. Currently, the
FSM model has 4 state maps, representing the normal operation of the
node [(*Mercantile*)](src/main/java/com/salesforce/apollo/choam/fsm/Combine.java),
block production [(*Earner*)](src/main/java/com/salesforce/apollo/choam/fsm/Driven.java),
the view rotation [(*Reconfigure*)](src/main/java/com/salesforce/apollo/choam/fsm/Reconfiguration.java)
and Genesis bootstrapping [(*BrickLayer*)](src/main/java/com/salesforce/apollo/choam/fsm/Genesis.java).
The view reconfiguration logic provides
dynamic rotation of view/committee members based on random cuts across the underlying context membership rings on the
view context ID (digest). This balances the load across all available members.

## Messaging

Client transactions are submitted to the current members of the group using Point to Point messaging. Consensus block
production
uses Ethereal Gossip and is reused for view change and genesis bootstrapping consensus. In
CHOAM, only a small subset of the total membership produces new blocks. Consequently, the other members of the CHOAM
must
somehow receive these blocks and do so reliably. The CHOAM group (context) uses a Reliable Broadcast from the
_membership_ module to reliably distribute the blocks to all live members using a 2/3+1 variation of the firefly's ring
calculation. This protocol's message buffer is bounded and garbage collected and efficient in dissemination. As it is
a garbage collected, bounded buffer broadcast, the messages will ultimately age out and discarded. As join and recovery
synchronization rely upon getting these messages, during periods of no transactions the last block is periodically
rebroadcast.
