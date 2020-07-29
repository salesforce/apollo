# Apollo
The Apollo project aims for a distributed ledger platform based on a sea of DAG nodes.  As such, this project combines several interesting technologies.  The underlying membership of Apollo is managed by the Fireflies secure communication layer.  The consensus layer is supplied by Avalanche.  An IPFS-esque DAG based DHT is provided in addition to DAG ledger state.

## Requirements
Apollo requires the JDK 11 [GraalVM](https://www.graalvm.org/).  To install this, see the [Getting Started](https://www.graalvm.org/getting-started/) section at the GraalVM site.  Install the GraalVM for your operating system.  When that is installed, use the _gu_ GraalVM tool to [install the WASM support](https://www.graalvm.org/docs/reference-manual/languages/wasm/).

    gu install wasm

Note that you may also need to perform the following on MacOS:

    xattr -d com.apple.quarantine $JAVA_HOME

After the installation, as the GraalVM has not yet been notorized on Catalina

Apollo also requires [Maven](https://maven.apache.org/) 3.6.1 and above.  

## Protocols
* [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) - byzantine tolerant secure membership and communications
    * Assumes byzantine members (and allows one to parameterize the system according to the probability of such).
    * Creates an overlay network in which each member gossips with the successor of the member in a ring.
    * The monitoring ring is capable of detecting member crashes (failures).
    * Reliable group message flooding.
* [Avalanche](https://arxiv.org/abs/1906.08936) - scalable, leaderless, byzantine fault tolerant consensus
    * Consensus on causal ordering of events with a high dynamic range of nodes.
* Ghost- a Merkle directed acyclic graph (DAG) used for storing block data
    * An immutable, content-based, single hop Distributed Hash Table (DHT) in which the "key" of data stored is the hash of that data (meaning you look up data with the key).
    * Leverages the underlying Fireflies rings for consistent hash rings and single hop routing.


## Not A Coin Platform(tm)
Apollo isn't designed for coins, rather as essentially a distributed database.  Of course the systems of Apollo can be used for such, the design goals are much different.  Thus, no coins for you.


## Status
Note that Apollo is very much a _work in progress_.  It is by no means a full featured, hardened distributed ledger platform.  I am a strong believer in iterative development and believe it is the only way to create robust systems.  Consequently, things are still changing, there is much experimental in the current work, and much left to be done. ;)


## Requirements
Apollo is a pure Java application  The build system uses Maven, and requires Maven 3.6.1+.  The Maven enforcer plugin enforces dependency convergance and Apollo is built using __Java 11__, in particular using the _GraalVM_.

Apollo is a [multi module Maven project](https://maven.apache.org/guides/mini/guide-multiple-modules.html).  This means that the various modules of Apollo are built and versioned as a whole, rather than being seperated out into individual repositories.  This also means that modules refer to other modules within the project as dependencies, and consequently must be built in the correct order.  Note that Maven does this by default, so there should be no issues.  However, it does mean that you can't simply cd into a module and build it without building its dependencies first.


## Building
To build Apollo, cd to the root directory of the repository and then do:
   
    mvn clean install

Note that the _install_ maven goal is **required**, as this installs the modules in your local repository for use by dependent modules within the rest of the build.
