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

__To Do__
* Handling certificate rotation in nodes
* Full generalization of key algorithms, hash and signing algorthm, etc
    * currently, the system is basically expecting RSA keys and such.  Hasn't been tested with ECC and other signinig algorithms.
* Handling of the certificate authority certificate rotation
    * this is a doozy, obviously. ;)

## API

Fireflies exposes the View, Member and Node as the fundamental abstractions.  The Fireflies rings are represented by the Ring abstraction and each view provides a access to the configured Rings and all the traversal mechanisms provided by such.  Several abstractions are provided to allow clients of the Apollo Fireflies substrate to "hook" into lifecycle and important transport events.  Clients making use of the FF substrate are designed to be driven by the round of the Fireflies Node for the Java process.  Given that much of FF is dependent on the 2 * T + 1 rings, this allows client layers to synchronize work with the underlying pulse of the system.  As the reliable message flooding is a value add of Apollo, the FF provides a simple channel oriented reliable broadcast API for both flooding - i.e. sending - and receiving messages within the group.