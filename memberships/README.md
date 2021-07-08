# Membership

## Fundamental membership in Apollo

This module defines the fundamental membership metaphore in Apollo.  Any actor within the system, real or virtual, is based on this package.  Membership is Context scoped, and Members may be part of many different Contexts.

This module also defines common ring and ordering behavior.  The ID of a member is the unique Digest identifier of the member, assigned on creation, stable throughout its lifecycle.  The Ring abstraction provides a consistent hashing ring where Members may be ordered according to the hash of the Member or specified Digest.  Each ring has a unique hash function that is distinct from any other ring in a Context.

The membership module also provides fundamental communication abstractions and implementations for both MTLS GRPC (HTTP/2) as well as local, in process emulations using in process communication facilities of GRPC.

This module also provides fundamental gossip based Messaging functionality within a Context.  The RingCommunications provides a general abstraction for traversing cuts across a Context's rings at a given Member or any supplied Digest.  Likewise a more specialized RingIterator is supplied for terminating actions that may require repeated rounds of gossip to satisfy.

In addition to a reliable broadcast messaging abstraction, this module also provides a form of Causal broadcast using the brilliant ideas of the [Bloom Clock](https://arxiv.org/abs/1905.13064).  In a nutshell, the Bloom Clock provides a form of [Vector Clock](https://en.wikipedia.org/wiki/Vector_clock) that is compact, accurate and static in size despite dynamic Membership changes.

## Context

A membership group is defined by the Context. The Context is an abstraction from the [Fireflies paper on byzantine fault tolerant membership overlays](https://ymsir.com/papers/fireflies-tocs.pdf).  The Context abstracts the concepts in the Fireflies model from any underlying membership maintenance mechanism, making it reusuable across features with different mechanisms and requirements.  Each Context has a Digest ID, which uniquely defines the context.  Contexts provide a set of consististent Rings that partition the membership in a random graph (with low diameter, usually close to 2).  Contexts may have offline and active members, but the Context only provides the base mechanism for managing the state of the member - offline and active, not the policy that manages the state as this must be supplied by another system using the context.  All active members are mapped to the Rings managed by the Context.  All offline members are removed from the Rings of the Context.

Although not enforced, the Context rings, by convention, have a cardinality = 2 x tolerance + 1.  That is, the number of rings is a function of the provided byzantine tolerance level as well as the cardinality of the Context membership.  Thus, when large changes of membership occur, the Rings may well need to be recalculated to preserve properties.

Contexts provide a sampling function, which will choose a random ring and then sample K members from that ring.  Predicate functionality allows selection and traversal strategies.  The ResevoirSampler, combined with complete membership access, random ring hashes, Predicate driven sampling, etc, provides the tools for a vast domain of sampling schemes, leveraged by Avalanche and Consortium (for example).

## Member

The fundamental Member of a Context.  Knows about its public key, Identity and some other useful behavior.  The Member model is evolving to use the Apollo Stereotomy module for all key and identity managment.  Currently this module is not fully integrated with Stereotomy and thus key and identity management is manual for the most part.

## Identity

Each Member has a unique identity.  This identity is represented by a Digest hash, which is guaranteed to label a unique Member in the system Context.

## Ring

A ring is a circular ordering of members that are mapped to the ring.  All the active Members in a Context are mapped to each Ring in the Context.  Each ring is numbered 0 - N, and for each Ring a different hash is used.  This means that each Ring, while containing the same set of Members, will place these members in a different orderings.

Rings provide convienent operations to navigate and route and search around the ring, providing prefix tree functionality as well as functioning as a consistent hashing ring of the active Member set of the Context.

## Common Communications

In Apollo, communications is tightly bound to the Context and thus the Member.  This module defines the common routing used within apollo to provide support for multiple instances of the protocols supported.
Router is the primary communicaations abstraction used by the system.  Local, in process communication emulation is provided by the LocalRouter.  Interprocess communication is through the MtlsRouter, to provide
foundational identity guarantees upper layers require.  The LocalRouter provides the same identity management provided by MTLS, making this a clean GRPC routable abstraction.

As connections are a thing, this package provides a ServerConnectionCache for managing system wide connections between nodes.  This is somewhat policy based, but errs on the side of "if you have to, you have to"
which can lead to connection drains.  Also, too, it relies on good behavior (releasing) for garbage collection and such, and so can also be abused.  Luckily all interaction in Apollo is essentially
event based, and so makes try{} finally {} easy to apply locally.

## Distributed Bloom Filter Based Set Reconcilliation

Currently, the messaging model provides only one replication mechanism using the [Distributed Bloom Filter](https://arxiv.org/abs/1910.07782).  This mechanism leverages the Fireflies gossip communication pattern to transmit small bloom filters representing the state at a particular member.   Currently, a 3 phase gossip is used, consisting of 2 rounds. 

  1. Originating member generates a random seed and uses this to construct a bloom filter with a high false positive rate to encode its state membership set and sends this to the gossiping partner.
  2. The Member receiving the gossip calculates the difference using the supplied bloom filter and sends up to the parameterized maximum number of missing elements (messages) from the receiver.  In addition to this state update, the member also generates a new random seed and constructs a Bloom Filter of its current state (with a high false positive rate) and includes this along with the update reply.
  3. The originating member updates its state from the reply and then uses the other party's bloom filter to calculate what the other Member is missing from their state from the supplied bloom filter and updates that Member (up to the parameterized maximum).

This replication is highly reliable and converges very quickly using very small bloom filters as the only overhead in steady state.  As the messages are garbage collected (see next section), this means that the replication set between the members is highly dynamic, implying a highly challenging environment for set replication using gossip.  The use of the Distributed Bloom filter scheme provides the balance between accuracy and completeness that is well suited to small(ish) buffer sizes and highly dynamic membership sets with high volumes of messaging.

## Reliable broadcast

This module provides messaging using a form of gossip based on the active Members of a Context, and the unique ring structure of the context.  This provides a reusable base broadcast mechanism that can be used with any Context view.

Apollo messaging also provides a messaging abstraction with a bounded buffer.  This is a garbage collected reliable broadcast with bounded message buffer and is based on the most excellent paper [Reducing noise in gossip-based reliable broadcast](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.575.3297).  Messages are garbage collected and the messaging sysem maintains the parameterized bound on the number of messages maximum in a node.

This garbage collection also leverages the known gossip communication pattern of the Fireflies Rings maintained by the Context.  Recall that the Fireflies constructed Rings of a Context follow the form: 2 x t + 1, where t is the number of failures tolerated to match the overall byantine parameters of the Context.  Due to the construction of the Fireflies Rings in the Context, the expected time required for a message from any member to another member is given by (2 x t + 2) x diameter, where the diameter is very close to 2 for the supplied construction method.

What this means is that the reliable broadcast mechanism can predict how long it has to wait before - with high probability - the message has been propagated to every Member of the Context.  Thus, we can use this TTL as the maximum "age" of the Message within the system.  On every gossip round, a message's age is incremented.  When messages are gossiped, the age of a message received is max merged with the currently stored message state on the receiver.  Messages are garbage collected when the age of the message is greater than the calculated Time To Live of the Context.

Thus, the life time of a message is tracked independent of the gossip interval - it's simply the number of gossip rounds modulo the TTL.  When the number of messages stored in a particular Member exceeds the parameterized buffer size, messages will be GC'd, starting with out of date messages (i.e. > maxAge) and progressing to older but still "live" messages to reach the parameterized buffer size.  Thus even with low buffer maximum sizes, reliable, causal broadcast can proceed even in the presence of byzantine adversaries without buffer overflow.

## Causal Broadcast
Apollo causal broadcast is built on top of the reliable broadcast mechanism. Causal broadcast delivers the messages within the Context members in the causally orderered, or "[happened before](https://en.wikipedia.org/wiki/Happened-before)", progression of each Member's view of the system.

The causal broadcast makes use of the the [Bloom Clock](https://arxiv.org/abs/1905.13064) abstraction.  Each Member has an internal clock that represents the observed flow of event hashes for that Member. In addition to the Bloom Clock, each Member also tracks a sequence number, incrementing it for each message.  The current combination of the Bloom Clock and the sequence number is stamped each Causal Message a Member sends.

Remember that the Bloom Clock is a form of a vector clock.  However, in Apollo causal broadcast this clock is unique to each Member, not shared across all members.  What is shared across all members are the hashes of the events (causal messages) that each member observes.

## Byzantine Causal Broacast With Bounded Buffers

Combining all of the features of this module results in a compact, extremely efficient, highly secure causal broadcast messaging platform that is byzantine fault tolerant and eventually consistent, regardless of the number of sybils in the system.

Apollo causal broadcast achieves this by:
  - The use of the Fireflies rings (calculated ring structure) provides an underlying gossiping message pattern that is inherently BFT, to any parameterized degree desired.
  - Using the Context Membership communications, MTLS, and mutually authenticated communications in general provided by Apollo ensures strong identity and standard cryptographic mechanisms to ensure that only Members of the Context are allowed to communicate with each other over strongly secured and mutually authenticated communication channels.
  - Causal ordering is maintained by the individual members, not globally, cryptograpically sealed in each CausalMessage sent.
  - Causal ordering depends on observing the events in each clock sent, so this and other properties combine to negate spamming DDOS, anti DDOS and other byanztine behavor across the system
  - Causal ordering is maintained by a Bloom Clock unique to each member, but based on Context wide observed event flow

The result is a byzantine fault tollerant, bounded buffer, causal broadcast.  Unlike other ideas, Apollo's version uses a bounded buffer of what amounts to an event DAG of previous messages.  This means that Apollow can garbage collect messages rather than keep them for all eternity, although storing them is always an option where that is required.  However, the use of the Bloom Clock provides a probabalistic equivalent of the Vector Clock, which provides us with a "happened before" without all the overhead and has a fixed size regardless of the dynamics of group membership.

## CausalMessage DAG

Given the above discussions, it should come as no suprise that the CausalMessage of this module is fundamentally a cryptographically authenticated Directed Acyclic Graph and naturally forms a distributed authenticated datastructure.

The CausalMessage is designed to be reused in other modules providing alternative mechanisms and systems leveraging causality.



