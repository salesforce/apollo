/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.salesforce.apollo.fireflies.Member;

/**
 * The Communications abstraction for the Avalance family of protocols.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public interface Communications {
    /**
     * The Sink of query responses a node has outstanding - a mailbox
     */
    interface Sink<T> {
        /**
         * Consume the response from a query
         * 
         * @param from
         *            - the address of the sender
         * @param response
         *            - the result of the query
         */
        void consume(Member from, T response);

        /**
         * @return the unique UUID of the receiver
         */
        UUID getId();

        /**
         * The action to take when the receiver has been timed out
         */
        void onTimeout();
    }

    /**
     * @return the local address of this Communications instance
     */
    Member address();

    /**
     * Set up a rendezvous for the sink
     * 
     * @param sink
     *            - the message drop
     * @param timeout
     *            - how long to wait
     * @param unit
     * @return - the receiver
     */
    <T> Communications await(Sink<T> sink, long timeout, TimeUnit unit);

    /**
     * Cancel the Sink with the supplied id
     * 
     * @param id
     * @return - the receiver
     */
    Communications cancel(UUID id);

    /**
     * @return the Set of addresses of all tracked members
     */
    Set<Member> getPopulation();

    /**
     * Send a message to a subset of members.
     * 
     * @param query
     *            - the query to send
     * @param to
     *            - the population of members to send to
     * @param sink
     *            - the id of the Sink for responses
     * @return - the receiver
     */
    <T> Communications query(Supplier<T> query, Collection<Member> to, UUID sink);
}
