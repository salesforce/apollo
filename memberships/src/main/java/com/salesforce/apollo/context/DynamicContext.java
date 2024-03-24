/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Provides a DynamicContext for Membership and is uniquely identified by a Digest;. Members may be either active or
 * offline. The DynamicContext maintains a number of Rings (may be zero) that the DynamicContext provides for Firefly
 * type consistent hash ring ordering operators. Each ring has a unique hash of each individual member, and thus each
 * ring has a different ring order of the same membership set. Hashes for DynamicContext level operators include the ID
 * of the ring. Hashes computed and cached for each member, per ring include the ID of the enclosing DynamicContext.
 *
 * @author hal.hildebrand
 */
public interface DynamicContext<T extends Member> extends Context<T> {

    static <Z extends Member> Builder<Z> newBuilder() {
        return new Builder<>();
    }

    /**
     * Activate the supplied collection of members
     */
    void activate(Collection<T> activeMembers);

    /**
     * Mark a member as active in the context
     *
     * @return true if the member was previously inactive, false if currently active
     */
    boolean activate(T m);

    /**
     * Mark a member identified by the digest ID as active in the context
     *
     * @return true if the member was previously inactive, false if currently active
     * @throws NoSuchElementException - if no member is found in the context with the supplied ID
     */
    boolean activate(Digest id);

    /**
     * Mark a member as active in the context
     */
    boolean activateIfMember(T m);

    /**
     * @return the Stream of active members
     */
    Stream<T> active();

    /**
     * Answer the count of active members
     */
    int activeCount();

    /**
     * Answer the list of active members
     */
    List<T> activeMembers();

    /**
     * Add a collection of members in the offline state
     */
    <Q extends T> void add(Collection<Q> members);

    /**
     * Add a member in the offline state
     *
     * @return true if the member is newly added to the context
     */
    boolean add(T m);

    Context asStatic();

    /**
     * Clear all members from the receiver
     */
    void clear();

    /**
     * Deregister the membership listener identified by the supplied UUID
     */
    void deregister(UUID id);

    /**
     * Answer the active member having the id, or null if offline or non-existent
     */
    T getActiveMember(Digest memberID);

    /**
     * Answer the collection of offline members
     */
    Collection<T> getOffline();

    /**
     * Answer true if the member who's id is active
     */
    boolean isActive(Digest id);

    /**
     * Answer true if the member is active
     */
    boolean isActive(T m);

    /**
     * Answer true if a member who's id is the supplied digest is offline
     */
    boolean isOffline(Digest digest);

    /**
     * Answer true if a member is offline
     */
    boolean isOffline(T m);

    /**
     * Take the collection of members offline
     */
    <Q extends T> void offline(Collection<Q> members);

    /**
     * Take a member offline
     *
     * @return true if the member was active previously
     */
    boolean offline(T m);

    int offlineCount();

    /**
     * Take a member offline if already a member
     */
    void offlineIfMember(T m);

    /**
     * Rebalance the rings based on the current total membership cardinality
     */
    void rebalance();

    /**
     * Rebalance the rings to the new cardinality
     */
    void rebalance(int cardinality);

    /**
     * Register a listener for membership events, answer the UUID that identifies it
     */
    UUID register(MembershipListener<T> listener);

    /**
     * Remove the members from the context
     */
    <Q extends T> void remove(Collection<Q> members);

    /**
     * remove a member with the id from the receiving DynamicContext
     */
    void remove(Digest id);

    /**
     * remove a member from the receiving DynamicContext
     */
    void remove(T m);

    /**
     * @return the Stream of rings managed by the context
     */
    Stream<DynamicContextImpl.Ring<T>> rings();

    interface MembershipListener<T extends Member> {

        /**
         * A new member has recovered and is now active
         */
        default void active(T member) {
        }

        /**
         * A member is offline
         */
        default void offline(T member) {
        }
    }

    class Builder<Z extends Member> extends Context.Builder<Z> {

        @Override
        public DynamicContext<Z> build() {
            return new DynamicContextImpl<Z>(id, Math.max(bias + 1, cardinality), pByz, bias, epsilon);
        }
    }
}
