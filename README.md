# Apollo Delphinius
The Apollo Delphinius project aims for a distributed ledger platform based on a sea of DAG nodes.  As such, this project combines several interesting technologies.  The underlying membership of Apollo is managed by the Fireflies secure communication layer.  The consensus layer is supplied by a **GREEN** - i.e. non POW - and quiescent consensus protocol.

## Requirements
Apollo requires the JDK 17+.

Apollo also requires [Maven](https://maven.apache.org/) 3.6.3 and above.  

## Protocols
* Compact, self contained Crypto and Utility module - Self certifying, self describing Digests, Signatures and Identifiers 
* Stereotomy - Decentralized Identifier based foundation, based on [Key Event Receipt Protocol](https://github.com/decentralized-identity/keri) (KERI).
* MTLS network communication - Local communication simulation supported for trivial multiprocess simulation testing
* Group based protocol routing - multiple protocol instances per process
* [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) - byzantine tolerant secure membership and communications
    * Assumes byzantine members (and allows one to parameterize the system according to the probability of such).
    * Creates an overlay network in which each member gossips with the successor of the member in a ring.
    * The monitoring ring is capable of detecting member crashes (failures).
    * Reliable group message flooding.
* Reliable Broadcast - garbage collected group based reliable broadcast and sender total ordering
* CHOAM - dynamic committee based transaction ordering service producing linear logs - block chains
* SQL State - JDBC accessible SQL store backed materialized view evolved from CHOAM linear logs.  Supports DDL, Stored Procedures, functions.
* Ethereal: [Aleph BFT Consensus](https://arxiv.org/pdf/1908.05156.pdf) - Efficient atomic broacast in asynchronous networks with byzantine nodes
    * Consensus on causal ordering of transactions with a moderate dynamic range of nodes.
* Ghost- a Merkle directed acyclic graph (DAG) used for storing block data
    * An immutable, content-based, single hop Distributed Hash Table (DHT) in which the "key" of data stored is the hash of that data (meaning you look up data with the key).
    * Leverages the underlying Fireflies rings for consistent hash rings and single hop routing.


## Not A Coin Platform(tm)
Apollo isn't designed for coins, rather as essentially a distributed database.  Of course the systems of Apollo can be used for such, the design goals are much different.  Thus, no coins for you.


## WIP
Note that Apollo Delphinius is very much a _work in progress_.  There is not yet an official release.  Thus, it is by no means a full featured, hardened distributed ledger platform.  I am a strong believer in iterative development and believe it is the only way to create robust systems.

## Requirements
Apollo is a pure Java application  The build system uses Maven, and requires Maven 3.6.3+.  The Maven enforcer plugin enforces dependency convergance and Apollo is built using Java 17.

Apollo is a [multi module Maven project](https://maven.apache.org/guides/mini/guide-multiple-modules.html).  This means that the various modules of Apollo are built and versioned as a whole, rather than being seperated out into individual repositories.  This also means that modules refer to other modules within the project as dependencies, and consequently must be built in the correct order.  Note that Maven does this by default, so there should be no issues.  However, it does mean that you can't simply cd into a module and build it without building its dependencies first.


## Building
To build Apollo, cd to the root directory of the repository and then do:
   
    mvn clean install

Note that the  _install_  maven goal is **required**, as this installs the modules in your local repository for use by dependent modules within the rest of the build.  You must have invoked maven on the Apollo project root with the "install" goal at least once, to correctly build any arbitrary submodule.

## Flappers Ahead!

I still have a tiny handful of extremely annoying flappers in the full build cycle.  My profuse apologies. My aim is a clean, reproducable build for your consideration, and rest assured I am striving to achieve this.  Sadly, there are some still unfixable flappers that seem to only occur when running the full Maven build from the command line.  Again, I apologize and recommend resuming (mvn -rf <module>) from the failed modules.  Hopefully these flappers will be squashed with alacraty and order restored across the land ;)

## !! Unfortunate Platform Dependency !!
I develop on an M1 MBP, and there is (currently) no grpc compiler for this platform.  Consequently, the grpc compiler is hard wired for intel.  Apologies.  This will be fixed to be platform independent when this required artifact appears from Google, or I figure out how to special case the M1 platform with the Maven OS config stuff.

## Current Status
Currently, the system is in heavy devlopment.  Fundamental identity and digest/signature/pubKey encodings has been integrated.  Apollo is now using Aleph-BFT instead of Avalanche for consensus, in the form of the Ethereal module.  CHOAM has now replaced Consortium, and the SQL replicated state machine now uses CHOAM for it's linear log and transaction model.

The following modules have been disabled:

 * avalanche
 * consortium
 * apollo
 * apollo-web
 * func-testing
 * model

Most are legacy and will be deprecated, others are elided as the underlying model is developed.