# Stereotomy

The Stereotomy module provides base trust and identifiers for the rest of Apollo. Identifiers are autonomic, self
describing, self certifying and decentralized.

## Based on KERI

Stereotomy is a faithful implementation of
the [Key Event Receipt Infrastructure, or KERI](https://github.com/decentralized-identity/keri). This implementation
uses Protobuf serializations for the key events.

## Stereotomy Controller

Like most of the KERI implementations, the Stereotomy controller is the point of entry for entities that control keys n'
such. It is largely a policy enforcement via APIs for managing identifiers, signing, issuing, etc.

## Status

The Inception, Rotation and Interaction KERI events are implemented and minimally tested. The Stereotomy class provides
a minimal KERI controller prototype, operating in direct mode (i.e., writes to its own maintained logs).
The Current state includes the KERI Verifier and Validator implementations, as well as persistent (or in memory) key
state
store - KERI's version of the chain state of KERI events. Key management isn't sorted yet, as I need
to integrate PKCS11 and other models. The Stereotomy controller model is primitive and isn't appropriate for personal
use points, etc., and so refactoring the kernel out to support multiple use cases will continue.

## Implementation

Stereotomy is loosely based on the design of the foundation Java implementation of KERI.
Stereotomy uses protobuf to encode and represent KERI events.
Rather than focus on a JSON implementation of KERI events, Stereotomy
leverages protobuf for efficient implementation and cryptographic processing.  
