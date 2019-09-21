# Apollo Ghost

Apollo Ghost is a butt simple Distributed Hash Table (DHT).  The contents of Ghost are addressed by the content's hash.  The contents of Ghost consists of DAG nodes, and may reference other content entries through links.  The contents of Ghost thus form a Merkle DAG and is an authenticated data structure.

Apollo Ghost leverages the underlying Fireflies membership rings, as suggested in the [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) paper.  Ghost simply uses the hash of the entry and maps this to each of the underlying Fireflies rings, using them as a consistent hash ring to determine the members responsible for storing the content.  This makes Ghost a "one hop" DHT, meaning that - unlike most DHTs we're familiar with - there is no routing layer in Ghost.  It's simply not needed, as the TLS endpoint for the responsible node is determined locally from the Fireflies membership and subsequent mapping to any given ring.  Thus, unless the content is locally stored on the the querying node, a direct network connect can be made without redirecting through a routing network.

The reuse of the Fireflies layer is a tradeoff.  However, as Apollo is based on Fireflies, there's little incentive not to leverage the functionality for Ghost.  This simplifies things quite a bit, obviously, and at least for the basic functionality, there's really not much complexity at all.

## Status
Apollo ghost is a proof of concept only, and hasn't been even used mildly in anger.  Rather it's necessary functionality for later developments in the stack.  It was pretty easy to throw together, and certainly the gossip rebalancing needs a bit more work and certainly testing.  Was definitely worthwhile proving things out for Fireflies rings leveraging.
