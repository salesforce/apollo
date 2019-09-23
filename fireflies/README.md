# Apollo Fireflies

_The bioluminescent family of winged beetles model not only the on/off behavior of members, but like Byzantine members they are also known for their aggressive mimicry in order to dupe and devour related species._

---
Apollo Fireflies is based on the most excellent paper [Fireflies: A Secure and Scalable Membership and Gossip Service](https://ymsir.com/papers/fireflies-tocs.pdf).  Fireflies provides the base secure communications system for the rest of the Apollo stack.

## Status

Apollo Fireflies is mostly functionally complete, but certainly quite a bit more scale testing and hardening.  As Fireflies provides the secure communication base for the rest of the Apollo stack, the framwork gets quite a workout throughout the stack.

__Current Functionality__
* PKI integration
    * Requires a central certificate authority
    * Node ids managed through node certificates
* Gossip communications overlay
    * Reliable message flooding
    * Fireflies rings for consistent hashing
* Reliable broadcast
    * Garbage collected reliable broadcast with bounded message buffer based on the most excellent paper [Reducing noise in gossip-based reliable broadcast](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.575.3297)

__To Do__
* Handling certificate rotation in nodes
* Full generalization of key algorithms, hash and signing algorthm, etc
    * currently, the system is basically expecting RSA keys and such.  Hasn't been tested with ECC and other signinig algorithms.
* Handling of the certificate authority certificate rotation
    * this is a doozy, obviously. ;)
