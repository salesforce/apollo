# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is based on the most excellent paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).  Fireflies provides the base secure communications system for the rest of the Apollo stack.  This implementation relies upon the base  _Context_  and  _Member_  abstraction from the  *membership* module of Apollo.

## Design
The Fireflies implementation in Apollo differs significantly from the original paper. The Apollo implementation combines the ideas of the [original Fireflies paper](https://ymsir.com/papers/fireflies-tocs.pdf) as well as the additional ideas from two most excellent papers:
 * [Self-stabilizing and Byzantine-Tolerant Overlay Network](https://www.cs.huji.ac.il/~dolev/pubs/opodis07-DHR-fulltext.pdf)
 * [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf)

Apollo Fireflies provides a stable, virtually synchronous membership view in much the same fashion as the Rapid paper describes.  Membership is agreed upon across the group and changes only by consensus within the group.  This provides an incredibly stable foundation for utilizing the secure overlay that Fireflies provides.

The system is a pure gossip based system that is incredibly cheap and highly scalable.

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
