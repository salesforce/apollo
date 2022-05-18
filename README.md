# Apollo Delphinius
The Apollo Delphinius project is a multi-tenant, distibuted system platform. Apollo provides a secure communications overlay using Fireflies.  The consensus layer is supplied by an asynchronous bft consensus protocol. The sql state interface is via a JDBC connection over replicated SQL state machines, supported by checkpointed CHOAM linear logs. Identity and key managment is provided as a foundational service and integrated into the MTLS grpc communication.

The target service goal is a multitenant Zanzibar/KERI integration that provides a wide area replicated, low latency service for managing identity, key management, access control and verifiable credentials such as JWT issuance and validation.

## Build Status
![Build Status](https://github.com/salesforce/apollo/actions/workflows/maven.yml/badge.svg)

The Java Maven CI is now integrated, and given how weak these CI daemons are, this should guarantee reproducible clean builds from the command line maven.

## Building Apollo
To build Apollo, cd to the root directory of the repository and then do:
   
    mvn clean install

Note that the  _install_  maven goal is **required**, as this installs the modules in your local repository for use by dependent modules within the rest of the build.  You must have invoked maven on the Apollo project root with the "install" goal at least once, to correctly build any arbitrary submodule.

You can, of course, use the "--also-make-dependents" argument for maven "-amd" if you want to build a particular module without performing the full build.

## Requirements
Apollo requires the JDK 17+ and [Maven](https://maven.apache.org/) 3.8.1 and above

## Some Features
* Compact, self contained Crypto and Utility module - Self certifying, self describing Digests, Signatures and Identifiers as well as a generous sampling of solid Bloomfilters n cousins.
* Stereotomy - Decentralized Identifier based foundation and key managment infrastructure, based on the [Key Event Receipt Infrastructure](https://github.com/decentralized-identity/keri) (KERI).
* MTLS network communication - Can use KERI for certificate authentication and generation.  Local communication simulation, also, for simplified multinode simulation for single process (IDE) testing
* Multi instance GRPC service routing - Context keyed services and routing framework
* [Fireflies](https://ymsir.com/papers/fireflies-tocs.pdf) - byzantine tolerant secure membership and communications overlay
* Reliable Broadcast - garbage collected, context routed reliable broadcast
* Ethereal: [Aleph BFT Consensus](https://arxiv.org/pdf/1908.05156.pdf) - Efficient atomic broacast in asynchronous networks with byzantine nodes
* CHOAM - dynamic, committee based, transaction causal ordering service producing linear logs - Replicated State Machines, built on Ethereal.
* SQL State - JDBC accessible, SQL store backed, materialized view evolved from CHOAM linear logs.  Supports DDL, DML, stored procedures, functions and triggers.
* Delphinius - Google Zanzibar clone. Provides Relation Based Access Control hosted on CHOAM SQL state machines.

## Protobuf and GRPC
Apollo uses Protobuf for all serialization and GRPC for all interprocess communication.  This implies code generation.  Not something I adore, but not much choice in the matter.

## JOOQ
Apollo makes use of [JOOQ](https://www.jooq.org) as a SQL DSL for Java. This also implies code generation and, again, not something I adore, but...

## Not A Coin Platform(tm)
Apollo isn't designed for coins, rather as essentially a distributed multitenant database.  Of course, while the systems and mechanisms of Apollo can be used for such, the design goals are much different.  Thus, no coins for you.

## WIP
Note that Apollo Delphinius is very much a _work in progress_.  There is not yet an official release.  Thus, it is by no means a full featured, hardened distributed ledger platform.

## Requirements
Apollo is a pure Java application  The build system uses Maven, and requires Maven 3.8.1+.  The Maven enforcer plugin enforces dependency convergance and Apollo is built using Java 17.

Apollo is a [multi module Maven project](https://maven.apache.org/guides/mini/guide-multiple-modules.html).  This means that the various modules of Apollo are built and versioned as a whole, rather than being separated out into individual repositories.  This also means that modules refer to other modules within the project as dependencies, and consequently must be built in the correct order.  Note that Maven does this by default, so there should be no issues.  However, it does mean that one can't simply cd into a module and build it without building its dependencies first. If you feel you must do so, please make sure to include the "install" goal and please make sure you add the "--also-make-dependents" or "--amd" parameter to your maven invocation.

## Code Generation In Apollo
Apollo requires code generation as part of the build.  This is performed in the Maven "generate-sources" phase of the build.  Consequently, this build phase *must* be run at least once in order to generate the java sources required by the rest of the build.

The current code generators used in Apollo are GRPC/Proto and JOOQ.  GRPC is for the various serializable forms and network protocols used by Apollo.  The JOOQ code generation is for the JOOQ SQL functionality.

Code generation is output into the (module dir)/target/generated-sources directory.  For GRPC/Proto, there are 2 directory roots: "(module dir)/target/generated-sources/protobuf/grpc-java" and "(module dir)/target/generated-sources/protobuf/java".  For JOOQ, the root directory is "(module dir)/target/generated-sources/jooq".

Again, I stress that because these generated source directories are under the "(module dir)/target" directory, they are removed during the "clean" phase of Maven and consequently must be regenerated in order to compile the rest of the build.

Note that adding these generated source directories to the compile path is automatically taken care of in the Maven *pom.xml* in the "build-helper" plugin.

## IDE Integration
**This is Important!**
Apollo contains one module that create a shaded version of standard libraries.  This module **must** be built (installed), but only needs to be run once in order to install the resulting jar into your local maven repository.  Obviously, the also have to be built by Maven, and consequently it is part of the build sequence.  However, Eclipse and IntellJ **do not understand this**. What this means is that the IDE thinks the module is fine and doesn't notice there has been package rewriting to avoid conflicts with existing libraries.  What this means is that you *must* exclude this module in your IDE environment.  If you really think you need to be working on it, then you probably understand all this. But if you are simply trying to get Apollo into your IDE, importing these module is gonna ruin your day.

### Module to exclude

The module to exclude is:

 * h2-deterministic

Again, I stress that you must **NOT** include this in the import of Apollo into your IDE. You'll be scratching your head and yelling at me about uncompilable code and I will simply, calmly point you to this part of the readme file.

This modules must be built, so please run

    mvn clean install -DskipTests

From the command line before attempting to load the remaining Apollo modules into your IDE. Again, this only need be done once as this will be installed in your local Maven repository and you won't have to do it again.  Rebuilding this module will have no adverse effect on the rest of the build.

### Your IDE and Maven code generation

Due to the code generation requirements (really, I can't do jack about them, so complaining is silly), this can cause interesting issues with your IDE if you import Apollo.  I work with Eclipse, and things are relatively good with the current releases. However, there are sometimes synchronization issues in Eclipse Maven integration that may require an additional generate-sources pass. Apollo is a multi-module project and be sure you're leaving time for the asynchronous build process to complete.

I have no idea about IntellJ or Visual Code, so you're on your own there.

What I  _strongly_  recommend is first building from the command line with -DskipTests - i.e "mvn clean install -DskipTests".  This will ensure all dependencies are downloaded and all the code generation is complete. Further, if you haven't updated from this repo in a while, don't try to be clever.  Delete all the modules from this project from your ide, build/test from the command line and _then_ reimport things. Don't ask for trouble, I always say.

After you do this, you shouldn't have any issue *if* your IDE Maven integration knows about and takes care of using the build-helper plugin to manage compilation directories for the module in the IDE.  However....

Myself, I find that I have to first select the top level Apollo.app module, and then Menu -> Run As -> Maven generate sources (or the equivalent in your IDE).  This *should* generate all the sources required for every submodule, so...

Feel free to generate issues and such and I will look into it as I do want this to be flawless and a good experience.  I know that's impossible, but it undoubtedly can be made better, and PRs are of course a thing.

## Metrics
Apollo uses Dropwizard Metrics and these are available for Fireflies, Reliable Broadcast, Ethereal and CHOAM.

## Testing
By default, the build uses a reduced number of simulated clients for testing.  To enable the larger test suite, use the system property "large_tests".  For example

    mvn clean install -Dlarge_tests=true

This requires a decent amount of resources, using two orders of magnitude more simulated clients in the tests, with longer serial transaction chains per transactioneer client.  This runs fine on my Apple M1max, but this is a beefy machine.  YMMV.

## Current Status
Currently, the system is in devlopment.  Fundamental identity and digest/signature/pubKey encodings has been integrated.  Apollo is using Aleph-BFT for consensus, in the form of the Ethereal module.  CHOAM has now replaced Consortium, and the SQL replicated state machine now uses CHOAM for it's linear log and transaction model.

Multitenant shards is in place and being worked upon currently.  This integrates Stereotomy and Delphinius using CHOAM.  An E2E test of the ReBAC Delphinius service is in development being tested.  Full integration of ProcessDomains using Fireflies discovery is in development.


