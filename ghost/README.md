# Apollo Ghost

Apollo Ghost is a simple Distributed Hash Table (DHT) based on the Apollo Membership Context.  Ghost provides two distinct types of services: _immutable_ and _mutable_.  The first is storage of 
_immutable content_.  Immutable content means that
the value for a key will always be the same value. The immutable contents of Ghost is addressed by the content's hash - the key is the value's hash. This property makes it super simple to reliably store 
immutable content, as the store operation is idempotent. In the case of retrieval, the key is hashed digest of the value, so any of the members of the DHT that return a non null value that matches the 
hash key can supply a value - a majority of responses is not required.

Ghost also provides _mutable_ {key, value} bindings.  The _mutable_ content in Ghost is eventually consistent, as the _value_ stored for a key may be updated simultaneously across the distributed system - there is no
atomic update.  Consequently, when looking up a previously bound {key, value} pair, Ghost awaits storing members to respond and only returns a value if if a majority of them agree on the value.

Apollo Ghost leverages the underlying Membership rings, as suggested in the [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) paper.  Ghost simply uses the hash of the entry or the hash of the String key (for mutable
content) and maps this to each of the underlying Context rings, using them as a consistent hash ring to determine the members responsible for storing the content.  This makes Ghost a "one hop" DHT, meaning that - unlike most DHTs we're familiar with - there is no routing layer in Ghost.  It's simply not needed, as the TLS endpoint for the responsible node is determined locally from the Contex membership and subsequent mapping to any given ring.  Thus, unless the content is locally stored on the the querying node, a direct network connect can be made without redirecting through a routing network.

## Status
Back under active development as this is required for Sterotomy resolver services in Apollo.
