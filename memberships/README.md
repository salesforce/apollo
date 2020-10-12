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
