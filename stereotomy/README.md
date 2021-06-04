# Stereotomy

The Stereotomy module provides base trust and identifiers for the rest of Apollo.  Identifiers are self describing, self certifying and decentralized.

## Based on KERI

Stereotomy is a faithful imlementation of the [Key Event Receipt Infrastructure, or KERI](https://github.com/decentralized-identity/keri).  This implementation uses Protobuf serializations for the key events.

## Stereotomy Controller

Like most of the KERI imlementations, the Stereotomy controller is the point of entry for entities that control keys n' such.  It is largely a policy enforcement via APIs for managing identifiers, signing, issuing, etc.

## Transport

Currently, only GRPC transport is supported, integrated with Apollo protocols.
