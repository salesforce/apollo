# Apollo Protocols

Apollo currently uses Avro IPC for all network communication. The protols implemented by Apollo are quite simple, consisting of largely bytes, so the serialization speed and efficiency isn't an issue.  And I like the fact that Avro is easy and works for all my versioning scenarios.

This module implements the core communications abstraction used throughout the Apollo stack.  This module also provides an MTLS Avro _Server_ and outbound communications client used by other dependent modules.

## Status
As this is a base module reused in all layers of the stack, this gets a good workout.  The functional testing here is minimal (ha!) but the MTLS layer machinery gets a very good functional workout in the _Fireflies_ module, where we run a 100 member group with Fireflies.

I originally modified the Netty 3.x Avro Server and Transport, but found I really needed to move to 4.x because of TLS bugs in Java 11.  Also, too, because one really shouldn't be stuck in the stone age with this, so the MTLS and Transport Netty pieces have been rewritten, and a few bugs fixed.  Moar testing, obviously, for the edge cases omnipresent in TCP/M/TLS comms.
