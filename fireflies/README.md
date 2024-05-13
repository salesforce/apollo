# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members
they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is fundamentally based on the most excellent
paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).
Fireflies provides the base secure, byzantine intrusion tolerant communications overlay for the rest of the Apollo
stack. This implementation relies upon the base  _Context_  and  _Member_  abstraction from the  *membership* module of
Apollo.

## Design

The Fireflies implementation in Apollo differs significantly from the original paper. The Apollo implementation combines
the ideas of the [original Fireflies paper](https://ymsir.com/papers/fireflies-tocs.pdf) as well as the additional ideas
from two most excellent papers:

* [Self-stabilizing and Byzantine-Tolerant Overlay Network](https://www.cs.huji.ac.il/~dolev/pubs/opodis07-DHR-fulltext.pdf)
* [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf)

and synthesizes the three designs into a BFT stable Rapid like membership and secure overlay service.

### Stable, Virtually Synchronous View Membership Service

Apollo Fireflies provides a stable, virtually synchronous membership view in much the same fashion as the Rapid paper
describes. Membership is agreed upon across the group and changes only by consensus within the group. This provides an
incredibly stable foundation for utilizing the BFT secure overlay that Fireflies provides.

Members must formally join a context. This is a two phase protocol of first contacting a seed (any one will do) and then
following the redirection to further members of the context to await the next view that includes the joining member. To formally join
a context (cluster) of nodes, the joining member needs to know the complete membership of the context.  This membership 
can be quite large.  Thus, the membership of the view is discovered, rather than returned from the Join prototocol.  
The information supplied to the joining member in the Gateway of the join response from the
redirect member for the joining view (the seeding member of the view supplied by the first phase of the Join protocol).
The Gateway of this join contains a hash of the  _crown_  of the view (see view identity below) as well as a tight Bloom
Filter that defines the membership set. This scheme is based off the most excellent
paper [HEX-BLOOM: An Efficient Method for Authenticity and Integrity Verification in Privacy-preserving Computing](https://eprint.iacr.org/2021/773.pdf).

This Join protocol is scalable and reuses the underlying Fireflies state reconcilliation to fill out the remaining
membership using gossip across the membership. After the Join protocol succeeds, the member gossips with peers 
in the same view and participates in view changes. A member does not vote upon a view or accept Seed or Join 
requests until the view's membership is fully acquired.

### Byzantine Fault Tolerant Join Protocol

Apollo Fireflies extends the *Rapid* Join protocol into a BFT form. This fits naturally into the Fireflies model and
relies, of course, on the BFT ring structure of the Fireflies *View* context. When the joining member contacts a seed,
the redirect is to the BFT members of the seed's context, returning what would be the successors of the joining member
in the current context. The joining member must acquire a BFT majority agreement on the *crown* of the joining view and
the membership bloom filter that defines the membership set. This provides Apollo Fireflies with a byzantine fault
tolerand and secure Join protocol. Which is pretty cool.

### Gossip Optimized Join

Note that Apollo Fireflies does not return all the _SignedNotes_ (Apollo Fireflies KERI equivalent of an X509
certificate) of all the members known in a view when responding to the Join. During the Join protocol members may 
not exist in the joining member's context beyond the seed set and cannot be contacted. They are essentially in a
pending state where the joining member is awaiting the state transfer of the these members'  _SignedNotes_ . Upon
obtaining all members' notes the member then computes the  _crown_  of the membership and the view ID is the digest
value of  _crown.compact()_.

Apollo Fireflies reuses the  _redirect_  part of the original Fireflies protocol to redirect the joining members to
their correct successors in the current state of the view. The redirected joining member uses this discovery to fill out
it's membership in the context. The underlying gossip state replication provides the state transfer of all the member's
SignedNote state for the view to the joining member. This occurs rather quickly and spreads this rather large plug of
state transfer required by the join of a view across the system, rather than punishing a member or two haphazardly. 
It's also async, schocastic and BFT, which is nice.

### View Consensus Voting
After a member has joined, the member can then participate in the global consensus that determines the view membership
evolution. This process deviates significantly from
the [Rapid paper](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf).  In this implementation, there
is a distinct BFT subset of the View membership that are responsible for Observations - that is, the ballots for changing
a View from mebership set A to membership set B.  This set is chosen using the Context.bftSubset(digest) method and results
in a small number of members Fireflies delegates the view change voting responsibilities.

In response to a number of Joins or Leaves, a vote will be scheduled on each View.  Only the bft membership subset defined by
using the compact hash of the View's HexBloom crown may vote and only their votes are counted across the membership.  This
gives use a byzantine fault tolerant way of cheaply getting trustworthy votes.  When voting occurs, all members count the votes
and change the view accordingly, but only the bft meber subset voted.  Besides being quite secure and highly available, using this
scheme is incredibly cheap as this BFT Subset cardinality is way, way less - in general - than the cardinality of the group.

In the original Rapid paper, a 3/4+1 consensus is used and Fireflies had that for a while as well.  However, this would
not scale to the extent we would like - e.g. 1,000,000 members.  So using the BFT subset, we're reusing the fundamental
BFT Ring Structure of the Fireflies context  _itself_  which I also find highly satisfying.  So for 1,000,000 members
rather than having 750,000 ballots we can leverage the View Context's rings to compute the BFT subset and get away with
13-42 (depending on pByz one chooses).  This is a dramatic reduction both in state - we don't need 750K ballots to replicate
around to everyone, nor do we need to get them to all agree and count them up.  So the Voting protocol now seems
both secure and efficient.

Only incremental state transfer occurs for existing members during future joins, modulo any state reconcilliation
transfer for joining members. Steady state overhead is bounded and independent of the number of members in the view.
As this is all based on the underlying gossip protocol, and the network bandwidth and compute spread evenly across the system.

### Designed For Stablity

Apollo Fireflies leverages the same sort of  __"almost everywhere"__  agreement that  _Rapid_  provides. Rather than
using a clever windowing proxy for stability as Rapid does, Apollo reuses the Accusation mechanism of the Fireflies
protocol. Member  _instability_  is defined as existing  _rebuttal timers_  when the vote is proposed. If there are no
rebuttal timers, the View is considered to be in a  _stable_  state for the member. When a member is stable, and if
there are non zero joining/leavings, and the member is an Observer (member of the BFT subset), then a  _Vote_  on 
the new membership view is created and submitted. After the vote is submitted, the member awaits the fast path 
consensus on the membership.

#### Shunning

Apollo Fireflies also differs from the original Fireflies by enforcing the _shunning_ of members that have failed. In
the original protocol, members can come back to life after failing to rebut accusations if they issue a new note. In
Apollo Fireflies, like Rapid, the failing member is shunned after failing to rebut and must rejoin the system again.

#### Amplification

Apollo also implements the same  _amplification_  strategy of Rapid. When a member fails to rebut an accusation, the
member is now _shunned_ and will not be responded to. All members that witness the failing member that are assigned as
monitors of the failing member will issue  __additional__  Accusations if they have not already done so. This positive
feedback reinforces the consensus decision that the member is dead, Jim, and drives the system to a common decision
quite quickly.

## View Identity

Views are membership groups and are identified by a hash (32 bytes by default). The  _crown_  of a View in Apollo
Fireflies is a HexBloom (see the cryptography module) and is defined by the set of digest IDs of the total membership
XORd together. The  _identity_  of the view is the public rehashing of the crowns of the HexBloom. This final rehashing
of the crown becomes the View ID. The integration of HexBloom into the calculation of the view id ensures that
individual membership identities are authenticated in aggregate simply and compactly and validated with a strong binding
through rehashing to produce the view identifier. Group membership is strongly bound to the  _identifier_  of the View.
When the group membership set changes - i.e. members joining or leaving - the view identifier also changes.

## Liveness, Failure Detection and Monitoring

Unlike other popular solutions, Apollo Fireflies does not have a separate monitoring  _ping_  failure detection
protocol. For example, the Fireflies papers and Rapid both use a separate monitoring  _ping_  for failure detection.
Apollo Fireflies depends on the continual gossip with partners and thus the monitored member is also the member gossiped
with. Apollo Fireflies combines these two logical operations into one. Therefore, a member is considered  _live_  if
they can complete gossip communication without failure or exception.

Note that  _failure_  and  _liveness_  now include understanding the state of consensus membership. Views that do not
share a common view identity do not communicate with each other and will treat this as a failure and thus accuse
appropriately. Likewise when a member is shunned, or the receiving member is in the process of joining a view.

This is a crisp definition of  _liveness_  that also matches the functional operation of the protocol. This does mean
that, unlike Rapid, Apollo Fireflies cannot (currently) monitor non Fireflies members; Apollo requires gossip with
monitored members to ensure liveness. Apollo Fireflies view membership maintenance protocol is intimately tied to the
functional use of the system, so this makes perfect sense to me. Also, Apollo Fireflies operates over GRPC, which itself
does  _liveness_  pings, etc. So Apollo Fireflies merely leverages this existing communications infrastructure to elide
the necessity of a separate monitoring protocol.

## Current Limitations

The Fireflies module is functionally complete. Full bootstrap and join integration with Thoth is complete and tested.

## Status

Apollo Fireflies is functionally complete and is MVP, but certainly quite a bit more scale testing and hardening is
always in order.

__Current Functionality__

* Byzantine intrusion tolerant secure communications overlay
* Robust, stable, virtually synchronous view membership
* Incredibly cheap, highly scalable gossip-based implementation
* PKI integration
    * Pluggable authentication. KERI-based authorization as well as simple, trad CA authentication.
    * Node identity, validation, and key management managed via Stereotomy Identifiers
* Certificate key rotation
    * provided by Stereotomy key rotation facilities
* Identity bootstrapping
    * provided by Gorgoneion module
* Full generalization of key algorithms, hash and signing algorthm, etc
    * provided by Stereotomy and the crypto utils
