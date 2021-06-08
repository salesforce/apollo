# Membership

## Fundamental membership in Apollo

This module defines the fundamental Node membership.  Any actor within the system, real or virtual, is based on this package.  This package provides certificate based identity and public/private key functionality.
This module also defines common ring and ordering behavior.  The ID of a member is the unique hash identifier of the member, assigned on creation, stable throughout its lifecycle.  The Ring abstraction
provides a circular ring where members can be ordered according to the HASH of the member.  Each ring has a unique hash that is distinct
from any other labeled ring for each unique member mapped to the ring.

## Context

A membership group is defined by the _Context_.  This is an abstraction from the Fire Flies paper on byzantine fault tolerant membership overlays.  The Context abstracts this from any underlying mechanism
making it reusuable in other features.  Each _Context_ has a 256 bit ID, which defines the context.  Contexts provide the hashing map for the _Rings_ that partition the membership.  Contexts may have offline and active
members, but only provide mechanism for managing it, not the policy which must be supplied by another system.  All active members are mapped to the Rings managed by the Context.  Although not enforced, the Context
rings, by convention, have a cardinality = 2 x tolerance + 1.  That is, if one determines the system can survive 4 simultaneous failures, then the number of rings in a Fireflies determined Context will be 2 x 4 + 1 = 9 rings.  Active members are mapped to each ring in the context, and each member has a unique hash ordering in the ring.  For each ring, the member's hash is a combination of the member's ID, the ring number, and the ID of the context.

Contexts provide a sampling function, which will choose a random ring and then sample K members from that ring.  Predicate functionality allows selection and traversal strategies.  The ResevoirSampler, combined with complete membership access, random ring hashes, Predicate driven sampling, etc, provides the tools for a vast domain of sampling schemes, leveraged by Avalanche and Consortium (for example).

## Member

The fundamental Member of a Context.  Knows about its public key, Identity and some other useful behavior.  May not last long, but seems tenacious.

## Identity

Each Member has a unique identity.  This identity is represented by a 256 bit hash, which is guaranteed to label a unique Member in the system Context.

## Ring

A ring is a circular ordering of members that are mapped to the ring.  Each ring has a label and the HASH key used for a member is the ring label XOR the UUID of the member.  Members are sorted on the
ring in increasing BYTE order of the ring HASH of a given member.

Rings provide convienent operations to navigate and route and search around the ring, providing prefix tree functionality.

## Common Communications

In Apollo, communications is tightly bound to the _Context_ and thus the _Member_.  This module defines the common routing used within apollo to provide support for multiple instances of the protocols supported.
_Router_ is the primary communicaations abstraction used by the system.  Local, in process communication emulation is provided by the _LocalRouter_.  Interprocess communication is through the MtlsRouter, to provide
foundational identity guarantees upper layers require.  The LocalRouter provides the same identity management provided by MTLS, making this a clean GRPC routable abstraction.

As connections are a thing, this package provides a _ServerConnectionCache_ for managing system wide connections between nodes.  This is somewhat policy based, but errs on the side of "if you have to, you have to"
which can lead to connection drains.  Also, too, it relies on good behavior (releasing) for garbage collection and such, and so can also be abused.  Luckily all interaction in Apollo is essentially
event based, and so makes try{} finally {} easy to apply locally.

## Messaging

This module also provides a messaging gossip based on the context, and its ring structure.  This provides a reusable mechanism that can be used with any Context view and provides:
* Reliable broadcast
    * Garbage collected reliable broadcast with bounded message buffer based on the most excellent paper [Reducing noise in gossip-based reliable broadcast](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.575.3297)
* Member ordering broadcast
    * Messages broadcast from a member are delivered in order, with potential drops, to every member.  Message gap delivery is settled every TTL of the enclosing context's view.  Messages by members are not ordered with respect to each other.
