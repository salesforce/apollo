# Apollo Protocols

Apollo uses GRPC for all network communication. The protocols implemented by Apollo are quite simple, consisting of
largely bytes, so the serialization speed and efficiency isn't particularly an issue.

This module implements the core communications abstraction used throughout the Apollo stack. This module also provides
an MTLS  _Server_  and outbound communications client used by other dependent modules.

## Status

As this is a base module reused in all layers of the stack, this gets a good workout. The functional testing here is
minimal (ha!) but the MTLS layer machinery gets a very good functional workout in the _Fireflies_ module, where we run a
100 member group with Fireflies.

GRPC naturally multiplexes client and server communications, so all communications go through one connection now, rather
than one per protocol.
