# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is fundamentally based on the most excellent paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).  Fireflies provides the base secure, byzantine intrusion tolerant communications overlay for the rest of the Apollo stack.  This implementation relies upon the base  _Context_  and  _Member_  abstraction from the  *membership* module of Apollo.

## Design
The Fireflies implementation in Apollo differs significantly from the original paper. The Apollo implementation combines the ideas of the [original Fireflies paper](https://ymsir.com/papers/fireflies-tocs.pdf) as well as the additional ideas from two most excellent papers:
 * [Self-stabilizing and Byzantine-Tolerant Overlay Network](https://www.cs.huji.ac.il/~dolev/pubs/opodis07-DHR-fulltext.pdf)
 * [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf)

and synthesizes the three into a BFT stable Rapid like membership.

### Stable, Virtually Synchronous View Membership Service
Apollo Fireflies provides a stable, virtually synchronous membership view in much the same fashion as the Rapid paper describes.  Membership is agreed upon across the group and changes only by consensus within the group.  This provides an incredibly stable foundation for utilizing the BFT secure overlay that Fireflies provides.

Members must formally join a context.  This is a two phase protocol of first contacting a seed (any one will do) and then redirecting to further members of the context to await the next view that includes the joining member.  To join a context (cluster) of nodes, the joining member needs to know the complete membership of the context. The memship of the view is discovered using the information supplied to the joining member in the Gateway of the join response from the redirect member for the joining view (the seeding member of the view supplied by the first phase of the Join protocol).  The Gateway of this join  contains a hash of the  _crown_  of the view (see view identity below) as well as a tight Bloom Filter that defines the membership set.  This scheme is based off the most excellent paper [HEX-BLOOM: An Efficient Method for Authenticity and Integrity Verification in Privacy-preserving Computing](https://eprint.iacr.org/2021/773.pdf).

This Join protocol is scalable and reuses the underlying Fireflies state reconcilliation to fill out the remaining membership.  After the Join protocol succeeds, the member gossips with peers in the same view and participates in view changes.  A member does not vote upon a view or accept Seed or Join requests until the view's membership is fully acquired.  

### Gossip Optimized Join
Apollo Fireflies does not return all the _SignedNotes_ (Apollo Fireflies KERI equivalent of an X509 certificate, as in the origina FF paper) for all the members known in a view.  Rather in the Join protocol, most members may not exist in the joining member's context and thus cannot be contacted.  They are essentially in a pending state  where the joining member is awaiting the state transfer of the these members' _SignedNotes_.  Upon obtaining all members' notes the member computes the  _crown_  of the membership and the view ID is the digest value of  _crown.rehash()_.

Apollo Fireflies reuses the _redirect_ part of the original Fireflies protocol to redirect the joining members to their correct successors in the current state of the view.  The redirected joining member uses this discovery to fill out it's membership in the context. The underlying gossip state replication provides the state transfer  of all the member's SignedNote state for the view to the joining member.  This occurs rather quickly and spreads this rather large plug of state transfer required in the join of a view across the system, rather than punishing a member or two.  It's also async, schocastic and BFT, which is nice.

After a member has joined, the member participates in the global consensus that determines the view membership evolution.  This process follows the broad outlines of the [Rapid paper](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf).  Periodically the group members will issue votes on what they believe the next view membership should be.  This is a list of SignedNotes of joining members and the digests of leaving members.  When a member acquires 3/4+1 votes, and 3/4+1 of these votes agree on the joining and leaving members set, a new view will be installed in the member.

Thus, only incremental state transfer occurs for existing members during future joins, modulo any state reconcilliation transfer for joining members. Steady state overhead is bounded and linear with the number of members in the view.

### Designed For Stablity
Apollo Fireflies leverages the same sort of  __"almost everywhere"__  agreement that _Rapid_ provides. Rather than using a clever windowing proxy for stability as Rapid does, Apollo  reuses the Accusation mechanism of the Fireflies protocol.  Member  _instability_  is defined as existing  _rebuttal timers_  when the vote is proposed.  If there are no rebuttal timers, the View is considered to be in a  _stable_  state for the member.  When a member is stable, and if there are non zero joining/leavings, a  _Vote_  on the new membership view is created and submitted.  After the vote is submitted, the member awaits the fast path consensus on the membership.

_slow path consensus not implemented yet_

#### Shunning

Apollo Fireflies also differs from the original Fireflies by enforcing the _shunning_ of members that have failed.  In the original protocol, members can come back to life after failing to rebutt accusations if they issue a new note.  In Apollo Fireflies, like Rapid, the failing member is shunned after failing to rebutt and must rejoin the system again.

#### Amplification

Apollo also implements the same _amplification_ strategy of Rapid.  When a member fails to rebutt an accusation, the meber is now _shunned_ and will not be responded to. All members that witness the failing memter that are assigned as monitors of the failing member will issue __additional__ Accusations if they have not already done so.  This positive feedback reinforces the consensus decision that the member is dead, Jim, and drives the system to a common decision quite quickly.

## View Identity

The  _crown_  of a View in Apollo Fireflies is defined by the set of digest IDs of the total membership XORd together.  The  _identity_  of the view is the digest value of  _crown.rehash()_  .

## Liveness, Failure Detection and Monitoring

Unlike other popular solutions, Apollo Fireflies does not have a separate monitoring  _ping_  failure detection protocol. For example, the Fireflies papers and Rapid both use a separate monitoring  _ping_  for failure detection.  Apollo Fireflies depends on the continual gossip with partners and thus the monitored member is also the member that is gossiped with.  Apollo Fireflies combines these two logical operations into one.  Therefore, a member is considered  _live_  if they can complete gossip communication without failure or exception.

Note that  _failure_  and  _liveness_  now include understanding the state of consensus membership.  Views that do not share a common view identity do not communicate with each other and will treat this as a failure and thus accuse appropriately.  Likewise when a member is shunned, or the receiving member is in the process of joining a view.

This is a crisp defintion of  _liveness_  that also matches the functional operation of the protocol.  This does mean that, unlike Rapid, Apollo Fireflies cannot (currently) monitor non Fireflies members; Apollo requires gossip with monitored members to ensure liveness.  Apollo Fireflies view membership maintenance protocol is intimately tied to the functional use of the system, so this makes perfect sense to me.  Also, Apollo Fireflies operates over GRPC, which itself does  _liveness_  pings, etc. So Apollo Fireflies merely leverages this existing communications infrasture to elide the necessity of a seperate monitoring protocol.

## Current Limitations

The system is mostly complete. Currently, only the super majority fast path consensus is implemented. Fall back consensus to appear at a later date.  Full bootstrap and join integration with Thoth pending.

## Status

Apollo Fireflies is mostly functionally complete and is MVP, but certainly quite a bit more scale testing and hardening is in order.  As Fireflies provides the secure communication base for the rest of the Apollo stack.

__Current Functionality__
* Byzantine intrusion tolerant secure communications overlay
* Robust, stable, virtually synchronous view membership
* Incredibly cheap, highly scalable gossip based implementation
* PKI integration
    * Uses pluggable authentication. KERI based authorization as well as simple, trad CA authentication.
    * Node identity, validation and key managment managed via Stereotomy Identifiers
* Certificate key rotation
    * provided by Stereotomy key rotation facilities
* Full generalization of key algorithms, hash and signing algorthm, etc
     * provided by Stereotomy and the crypto utils
* Handling of the certificate authority certificate rotation
    * provided by Stereotomy key rotation facilities
