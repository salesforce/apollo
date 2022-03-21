# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is based on the most excellent paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).  Fireflies provides the base secure communications system for the rest of the Apollo stack.

## Status

Apollo Fireflies is mostly functionally complete, but certainly quite a bit more scale testing and hardening.  As Fireflies provides the secure communication base for the rest of the Apollo stack, the framwork gets quite a workout throughout the stack.

__Current Functionality__
* PKI integration
    * Uses pluggable authentication. KERI based authorization as well as standard PKI CA authentication.
    * Node identity and key managment managed via Stereotomy
* Gossip communications overlay
    * Reliable message flooding
    * Fireflies rings for consistent hashing
* Certificate key rotation
    * provided by Stereotomy key rotation facilities
* Full generalization of key algorithms, hash and signing algorthm, etc
     * provided by Stereotomy and the crypto utils
* Handling of the certificate authority certificate rotation
    * again provided by Stereotomy key rotation facilities
