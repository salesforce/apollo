# Genesis Apollo
The Apollo service is a distributed ledger based on a sea of DAG nodes.  The underlying membership is managed by the Fireflies secure communication layer.  Consensus is supplied by Avalanche.  An IPFS-esque DAG based DHT is provided in addition to DAG ledger state.

The operation of a node is the same regardless of whether an Apollo node is a single node or a member of a decentralized network.  The nature of the decentralized architecture dictates that building out a single node is required before building out any interactions between nodes.

## Protocols
* [Avalanche](https://avalabs.org/snow-avalanche.pdf) - scalable, leaderless, Byzantine Fault Tolerant consensus
  * Comes to consensus on causal ordering of events with a high dynamic range of nodes
* [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) - a gossip-based membership service
  * Assumes byzantine members (and allows one to parameterize the system according to the probability of such)
  * Creates an overlay network in which each member gossips with the successor of the member in a ring
  * The monitoring ring is capable of detecting member crashes (failures)
* [Ghost](https://eprint.iacr.org/2018/104.pdf) - a Merkle directed acyclic graph (DAG) used for storing block data
  * Uses an immutable, idempotent, content-based Distributed Hash Table (DHT) in which the "key" of data stored is the hash of that data (meaning you look up data with the key)

## Instructions
If you haven't already, you'll first want to configure your system for Maven development at Salesforce.  You'll do this by configuring your `~/.m2/settings.xml` file per [the instructions here](https://git.soma.salesforce.com/modularization-team/maven-settings).

You can then clone the Genesis Apollo Git repository to your local box.
```
git clone git@git.soma.salesforce.com:salesforce-blockchain/genesis-apollo.git
```

You're now ready to import the project into your IDE of choice.  Apollo requires JDK 11.  It's important to note though that you'll want to use Salesforce's version of Java that is provided with BLT.  If not, you'll encounter certificate trust issues with Nexus.  Example: `/Library/Java/JavaVirtualMachines/sfdc-openjdk_11.0.1.jdk/Contents/Home`.
