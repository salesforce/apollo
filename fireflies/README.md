# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is based on the most excellent paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).  Fireflies provides the base secure communications system for the rest of the Apollo stack.  This implementation relies upon the base  _Context_  and  _Member_  abstraction from the  *membership* module of Apollo.

## Design
The Fireflies implementation in Apollo differs significantly from the original paper. The Apollo implementation combines the ideas of the [original Fireflies paper](https://ymsir.com/papers/fireflies-tocs.pdf) as well as the additional ideas from two most excellent papers:
 * [Self-stabilizing and Byzantine-Tolerant Overlay Network](https://www.cs.huji.ac.il/~dolev/pubs/opodis07-DHR-fulltext.pdf)
 * [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf)

Apollo Fireflies provides a stable, virtually synchronous membership view in much the same fashion as the Rapid paper describes.  Membership is agreed upon across the group and changes only by consensus within the group.  This provides an incredibly stable foundation for utilizing the secure overlay that Fireflies provides.

Members must formally join a context.  This is a two phase protocol of first contacting a seed (any one will do) and then redirecting to further members of the context to await the next view that includes the joining member.  To join a context (cluster) of nodes, the joining member needs to know the complete membership of the context. This is transmitted in the Gateway of the join response from the redirect member for the joining view.  Currently, all view member IDs are returned, and these are at minimum 32 byte digests, so eventually scalability will play an outsized role here.

Apollo Fireflies does not, however, return all the _SignedNotes_ (Apollo Fireflies KERI equivalent of an X509 certificate, as in the origina FF paper) for all the members.  Rather in the join, most members in the context have no signed notes, and thus cannot be contacted.  They are essentially in a pending state, where the joining member is awaiting the state transfer of the these member's _SignedNotes_.

The system reuses the _redirect_ part of the original Fireflies protocol to both connect the joining members to their correct successors in the current state of the view, but also provide state transfer, of course, of all the member's SignedNote state for the view.  This occurs rather quickly and spreads this rather large plug of state transfer required in the join of a view across the system, rather than punishing a member or two.  It's also schocastic and BFT, which is nice.

After a member has joined, it participates in the global consensus that determines the view membership evolution.  This process follows the broad outlines of the [Rapid paper](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf).  Periodically the group members will issue votes on what they believe the next view membership should be.  This is a list of signed notes of joining members and the digests of leaving members.  When a member acquires 3/4+1 votes, and 3/4+1 votes agree on the joining and leaving members, a new view will be installed in the member.

Thus, only incremental state transfer occurs for existing members, modulo any state reconcilliation for joining members. Steady state overhead is bounded and linear with the number of members in the view.

This system provides the same sort of "almost everywhere" agreement that _Rapid_ provides. Rather than using a clever windowing proxy for stability, Apollo Fireflies reuses the Accusation mechanism of the Fireflies protocol.  If there are now _rebuttal timers_ running, then that is considered to be a Stable state for the member and if there are non zero joins/leavings, a Vote on the new membership view is submitted.

Apollo Fireflies also differs from the original Fireflies by enforcing the _shunning_ of members that have failed.  In the original protocol, members can come back to life if they issue a new note.  In Apollo Fireflies, like Rapid, the failing member is shunned and must rejoin the system again.  Apollo also implements the same _amplification_ notion of Rapid.  When a member fails to rebutt, the meber is now _shunned_ and won't be responded to. All members that witness this that also are assigned as monitors will issue additional Accusations if they have not already done so.  This reinforces the consensus decision that the member is dead, Jim.

## Liveness, Failure Detection and Monitoring

Unlike other popular solutions, Apollo Fireflies does not have a separate monitoring _ping_ failure detection protocol. For example, the Fireflies papers and Rapid all use a separate monitoring _ping_ for failure detection.  Apollo Fireflies depends on continual gossip and the monitored member is also the member that is gossiped with.  Apollo Fireflies combines these operations.  Therefore, a member is considered _live_ if they can complete gossip communication.

This is a crisp defintion of _liveness_ that also matches the functional operation of the protocol.  This does mean that, unlike Rapid, Apollo Fireflies cannot (currently) monitor non Fireflies members - it requires gossip with monitored members.  Apollo Fireflies view membership is intimately tied to the function of the system, so this makes perfect sense to me.  Also, Apollo Fireflies operates over GRPC, which itself does _liveness_ pings, etc. So Apollo Fireflies merely leverages this existing communications infrasture to elide the necessity of a seperate monitoring protocol.

## Current Limitations

The system is mostly complete. Currently, only the super majority fast path consensus is implemented. Fall back consensus to appear at a later date.  Full bootstrap and join integration with Thoth pending.

## Status

Apollo Fireflies is mostly functionally complete and is MVP, but certainly quite a bit more scale testing and hardening is in order.  As Fireflies provides the secure communication base for the rest of the Apollo stack.

__Current Functionality__
* Byzantine intrusion tolerant secure communications overlay
* Robust, stable, virtually synchronous view membership
* Incredibly cheap, highly scalable gossip based implementation
* PKI integration
    * Uses pluggable authentication. KERI based authorization as well as standard PKI CA authentication.
    * Node identity and key managment managed via Stereotomy
* Certificate key rotation
    * provided by Stereotomy key rotation facilities
* Full generalization of key algorithms, hash and signing algorthm, etc
     * provided by Stereotomy and the crypto utils
* Handling of the certificate authority certificate rotation
    * again provided by Stereotomy key rotation facilities
