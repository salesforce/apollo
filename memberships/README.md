# Membership

## Fundamental membership in Apollo

This module defines the fundamental Node membership.  Any actor within the system, real or virtual, is based on this package.  This package provides certificate based identity and public/private key functionality.
This module also defines common ring and ordering behavior.  The HASH of a member is the hash of it's certificate.  The Ring abstraction
provides a circular ring where members can be ordered according to the HASH of the member.  Each ring has a unique hash that is distinct
from any other labeled ring for each unique member mapped to the ring.

## Identity

Each Member has a unique identity.  This identity is represented by a UUID, which is guaranteed to label a unique Member in the system Context.

## Ring

A ring is a circular ordering of members that are mapped to the ring.  Each ring has a label and the HASH key used for a member is the ring label XOR the UUID of the member.  Members are sorted on the
ring in increasing BYTE order of the ring HASH of a given member.

Rings provide convienent operations to navigate and route and search around the ring, providing prefix tree functionality.

## Messaging

This module also provides a messaging gossip based on the context, and its ring structure.  This provides a reusable mechanism that can be used with any Context view and provides:
* Reliable broadcast
    * Garbage collected reliable broadcast with bounded message buffer based on the most excellent paper [Reducing noise in gossip-based reliable broadcast](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.575.3297)
* Member ordering broadcast
    * Messages broadcast from a member are delivered in order, with potential drops, to every member.  Message gap delivery is settled every TTL of the enclosing context's view.  Messages by members are not ordered with respect to each other.
