# Apollo Delphinius
The Apollo Delphinius project is an experimental multi-tenant, distributed system platform. Apollo provides a secure communications overlay using Fireflies.  The consensus layer is supplied by an asynchronous bft consensus protocol. The sql state interface is via a JDBC connection over replicated SQL state machines, supported by checkpointed CHOAM linear logs. Identity and key managment is provided as a foundational service and integrated into the MTLS grpc communication.

The target service goal is a multitenant Zanzibar/KERI integration that provides a wide area replicated, low latency service for managing identity, key management, access control and verifiable credentials such as JWT issuance and validation.

## Build Status
![Build Status](https://github.com/salesforce/apollo/actions/workflows/maven.yml/badge.svg)

The Java Maven CI is now integrated, and given how weak these CI daemons are, this should guarantee reproducible clean builds from the command line maven.

## Requirements
Apollo requires the [GraalVM](https://www.graalvm.org) JDK 17+ and [Maven](https://maven.apache.org/) 3.8.1 and above

### Install Maven
See [Installing Apache Maven](https://maven.apache.org/install.html) if you need to install Maven.

### Install GraalVM
Apollo now requires the GraalVM for Isolates and other fantastic features of the GraalVM.  To install the GraalVM, see the [Quick Start Guide](https://www.graalvm.org/java/quickstart/).  For Mac and Apple Silicon, use the [Homebrew Tap for GraalVM](https://github.com/graalvm/homebrew-tap).

## Building Apollo
**Important**: To provide deterministic SQL execution, Apollo requires an installation step that need only be done once.  If you are building Apollo for the first time, you  __must__  cd to the root directory of the repository and then:

    mvn clean install -Ppre -DskipTests
  
This will perform a full build, including the deterministic SQL execution module.  After this is complete, you do not need to do this again. You can build Apollo normally, without the deterministic SQL module and to do so cd to the root directory of the repository and then:
   
    mvn clean install

Note that the  _install_  maven goal is **required**, as this installs the modules in your local repository for use by dependent modules within the rest of the build.  You must have invoked maven on the Apollo project root with the "install" goal at least once, to correctly build any arbitrary submodule.

You can, of course, use the "--also-make-dependents" argument for maven "-amd" if you want to build a particular module without performing the full build.

## Some Features
* Compact, self contained Crypto and Utility module - Self certifying, self describing Digests, Signatures and Identifiers as well as a generous sampling of solid Bloomfilters n cousins.
* Stereotomy - Decentralized Identifier based foundation and key managment infrastructure, based on the [Key Event Receipt Infrastructure](https://github.com/decentralized-identity/keri) (KERI).
* PAL - Permissive Action Link implementation for attestation boostrapping secrets
* MTLS network communication - Can use KERI for certificate authentication and generation.  Local communication simulation, also, for simplified multinode simulation for single process (IDE) testing
* Multi instance GRPC service routing - Context keyed services and routing framework
* [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) - byzantine tolerant secure membership and communications overlay providing virtually synchronous, stable membership views.
* Efficient and easy to reuse utility patterns for Fireflies style gossiping on membership contexts
* Reliable Broadcast - garbage collected, context routed reliable broadcast
* Ethereal: [Aleph BFT Consensus](https://arxiv.org/pdf/1908.05156.pdf) - Efficient atomic broacast in asynchronous networks with byzantine nodes
* CHOAM - dynamic, committee based, transaction causal ordering service producing linear logs - Replicated State Machines, built on Ethereal.
* SQL State - JDBC accessible, SQL store backed, materialized view evolved from CHOAM linear logs.  Supports DDL, DML, stored procedures, functions and triggers.
* Delphinius - Google Zanzibar clone. Provides Relation Based Access Control hosted on CHOAM SQL state machines.

## Modules
Apollo is reasonably modularized mostly for the purpose of subsystem isolation and reuse.  Each module is a Maven module under the source root and contains a README.md documenting (such as it is at the moment, lol) the module.

* [CHOAM](choam/README.md) - Committee maintanence of replicated state machines
* [Delphinius](delphinius/README.md) - Bare bones Google Zanzibar clone
* [Demo](demo/README.md) - Hypothetical DropWizard REST API for Delphinus running on the Apollo stack
* [Ethereal](ethereal/README.md) - Aleph asynchronous BFT atomic broadcast (consensus block production)
* [Fireflies](fireflies/README.md) - Byzantine intrusion tolerant, virtually synchronous membership service and secure communications overlay
* [Deterministic H2](h2-deterministic) - Deterministic H2 SQL Database
* [Deterministic Liquibase](liquibase-deterministic) - Deterministic Liquibase
* [Memberships](memberships/README.md) - Fundamental membership and Context model. Local and MTLS GRPC _Routers_.  Ring communication and gossip patterns.
* [Model](model/README.md) - Replicated domains.  Process and multitentant sharding domains.
* [PAL](pal/README.md) - Permissive Action Link GRPC client, for secure bootstrapping of secets over unix domain sockets.
* [PAL-D](pal-d/README.md) - Permissive Action Link GRPC server, for secure bootstrapping of secets over unix domain sockets.
* [Protocols](protocols/README.md) - GRPC MTLS service fundamentals, Netflix GRPC and other rate limiters.
* [Schemas](schemas/README.md) - Liquibase SQL definitions for other modules
* [Sql-State](sql-state/README.md) - Replicated SQL state machines running on CHOAM linear logs.  JDBC interface.
* [Stereotomy](stereotomy/README.md) - Key Event Receipt Infrastructure.  KEL, KERL and other fundamental identity, key and trust management
* [Stereotomy Services](stereotomy-services) - GRPC services and protobuff interfaces for KERI services
* [Thoth](thoth/README.md) - Decentralized Stereotomy. Distributed hash table storage, protocols and API for managing KERI decentralized identity
* [Tron](tron/README.md) - Compact, sophisticated Finite State Machine model using Java Enums.
* [Utils](utils/README.md) - Base cryptography primitives and model. Bloom filters (of several varieties).  Some general utility stuff.


## Protobuf and GRPC
Apollo uses Protobuf for all serialization and GRPC for all interprocess communication.  This implies code generation.  Not something I adore, but not much choice in the matter. GRPC/Proto generation also appears not to play well with the Eclipse IDE Maven integration. To aleviate this,  _all_  grpc/proto generation occurs in one module, the aptly named  _grpc_  module.

## JOOQ
Apollo makes use of [JOOQ](https://www.jooq.org) as a SQL DSL for Java. This also implies code generation and, again, not something I adore.  Unlike GRPC, the JOOQ code generation plays very nicely with the Eclipse IDE's Maven integration, so JOOQ code generation is included in the module that defines it.

## Not A Coin Platform™
Apollo isn't designed for coins, rather as essentially a distributed multitenant database.  Of course, while the systems and mechanisms of Apollo can be used for such, the design goals are much different.  Thus, no coins for you.

## WIP
Note that Apollo Delphinius is very much a  _work_   _in_   _progress_ .  There is not yet an official release.  Thus, it is by no means a full featured, hardened distributed systems platform.

## Requirements
Apollo is a pure Java application  The build system uses Maven, and requires Maven 3.8.1+.  The Maven enforcer plugin enforces dependency convergance and Apollo is built using Java 17.

Apollo is a [multi module Maven project](https://maven.apache.org/guides/mini/guide-multiple-modules.html).  This means that the various modules of Apollo are built and versioned as a whole, rather than being separated out into individual repositories.  This also means that modules refer to other modules within the project as dependencies, and consequently must be built in the correct order.  Note that Maven does this by default, so there should be no issues.  However, it does mean that one can't simply cd into a module and build it without building its dependencies first. If you feel you must do so, please make sure to include the "install" goal and please make sure you add the "--also-make-dependents" or "--amd" parameter to your maven invocation.

## Code Generation In Apollo
Apollo requires code generation as part of the build.  This is performed in the Maven "generate-sources" phase of the build.  Consequently, this build phase *must* be run at least once in order to generate the java sources required by the rest of the build.

The current code generators used in Apollo are GRPC/Proto and JOOQ.  GRPC is for the various serializable forms and network protocols used by Apollo.  The JOOQ code generation is for the JOOQ SQL functionality.

GRPC/Protoc code generation only occurs in the _grpc_  module and is output into the  _grpc/target/generated-sources_  directory.  For GRPC/Proto, there are 2 directory roots:  _grpc/target/generated-sources/protobuf/grpc-java_  and  _grpc/target/generated-sources/protobuf/java_ .  For JOOQ, the root directory is  _(module dir)/target/generated-sources/jooq_ .

Again, I stress that because these generated source directories are under the "(module dir)/target" directory, they are removed during the "clean" phase of Maven and consequently must be regenerated in order to compile the rest of the build.

Note that adding these generated source directories to the compile path is automatically taken care of in the Maven *pom.xml* in the "build-helper" plugin.

## IDE Integration
**This is Important!**
Apollo contains one module that create a shaded version of standard libraries.  This module **must** be built (installed), but only needs to be built once in order to install the resulting jar into your local maven repository.  This is performed as part of the top level pom's  _pre_  profile.  As mentioned previously, this profile must be executed at least once before the full build.  Note, however, Eclipse and IntellJ **does not understand this transformation** and thus will not be able to import this module without errors and messing up the rest of the code that depends on the transformation. What this means is that the IDE thinks the module is fine and doesn't notice there has been package rewriting to avoid conflicts with existing libraries.  What this means is that you *must* exclude this module in your IDE environment.  This module will not be imported unless you explicitly do so, so please do not do so.  If you really think you need to be working on it, then you probably understand all this. But if you are simply trying to get Apollo into your IDE, importing these module is gonna ruin your day.

### Module to exclude

The module to exclude is:

 * h2-deterministic

Again, I stress that you must **NOT** include this in the import of Apollo into your IDE. You'll be scratching your head and yelling at me about uncompilable code and I will simply, calmly point you to this part of the readme file.

This module must be built, however, so please run the following once from the top level of the repository

    mvn clean install -Ppre -DskipTests

from the command line before attempting to load the remaining Apollo modules into your IDE. Again, this only need be done once as this will be installed in your local Maven repository and you won't have to do it again.  Rebuilding this module will have no adverse effect on the rest of the build.

### Eclipse M2E issues with ${os.detected.classifier}

This is a known weirdness with Eclipse M2E with the [os-maven-plugin build extension](https://github.com/trustin/os-maven-plugin).  I've been fine with this, but ran into another project that Eclipse just kept refusing to resolve.  I solved this by downloading the [supplied maven plugin](https://repo1.maven.org/maven2/kr/motd/maven/os-maven-plugin/1.7.0/os-maven-plugin-1.7.0.jar) and adding this to the **<ECLIPSE_HOME>/dropins** directory.  This works because the plugin is also an Eclipse plugin, which is nice.

### Your IDE and Maven code generation

Due to the code generation requirements (really, I can't do jack about them, so complaining is silly), the generation phase can occasionally cause interesting issues with your IDE whne you import Apollo.  I work with Eclipse, and things are relatively fine with the current releases. However, there are sometimes synchronization issues in Eclipse Maven integration that invalidates the generated code and that may require an additional *generate-sources* pass. Apollo is a multi-module project and be sure you're leaving time for the asynchronous build process to complete.

I have no idea about IntellJ or Visual Code, so you're on your own there.

What I  _strongly_  recommend is first building from the command line with **-DskipTests** - i.e **mvn clean install -DskipTests**.  This will ensure all dependencies are downloaded and all the code generation is complete. Further, if you haven't updated from this repo in a while, don't try to be clever.  Delete all the modules from this project from your ide, build/test from the command line and _then_ reimport things. Don't ask for trouble, I always say.

After you do this, you shouldn't have any issue *if* your IDE Maven integration knows about and takes care of using the build-helper plugin to manage compilation directories for the module in the IDE.  However....

Myself, I find that I have to first select the top level Apollo.app module, and then **Menu -> Run As -> Maven generate sources** (or the equivalent in your IDE).  This *should* generate all the sources required for every submodule, so...

Feel free to generate issues and such and I will look into it as I do want this to be flawless and a good experience.  I know that's impossible, but it undoubtedly can be made better, and PRs are of course a thing.

Note that also, for inexplicable reasons, Eclipse Maven will determine it needs to invalidate the _grpc_ generated code and will thus need to be regenerated. I'm trying to figure out the heck is going on, but when this happens please simply regenerate by selecting the _grpc_ module and performing: Menu -> Run As -> Maven generate sources (or the equivalent in your IDE).

## Metrics
Apollo uses Dropwizard Metrics and these are available for Fireflies, Reliable Broadcast, Ethereal and CHOAM.

## Testing
By default, the build uses a reduced number of simulated clients for testing.  To enable the larger test suite, use the system property "large_tests".  For example

    mvn clean install -Dlarge_tests=true

This requires a decent amount of resources, using two orders of magnitude more simulated clients in the tests, with longer serial transaction chains per transactioneer client.  This runs fine on my Apple M1max, but this is a beefy machine.  YMMV.

## Current Status
Currently, the system is in devlopment.  Fundamental identity and digest/signature/pubKey encodings has been integrated.  Apollo is using Aleph-BFT for consensus, in the form of the Ethereal module.  CHOAM has now replaced Consortium, and the SQL replicated state machine now uses CHOAM for it's linear log and transaction model.

Multitenant shards is in place and being worked upon currently.  This integrates Stereotomy and Delphinius using CHOAM.  An E2E test of the ReBAC Delphinius service is in development being tested.  Full integration of ProcessDomains using Fireflies discovery is in development.


