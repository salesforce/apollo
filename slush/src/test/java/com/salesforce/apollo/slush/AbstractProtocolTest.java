/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

@SuppressWarnings("rawtypes")
abstract public class AbstractProtocolTest {

    public AbstractProtocolTest() {
        super();
    }

    @Test
    public void diffusion() throws Exception {
        int cardinality = 10;
        Random entropy = new Random(0x666);

        List<MockCommunications> coms = new ArrayList<>();
        for (int port = 0; port < cardinality; port++) {
            Color initialColor = entropy.nextBoolean() ? null : entropy.nextBoolean() ? Color.Blue : Color.Red;
            coms.add(newMember(port, initialColor, cardinality));
        }
        coms.forEach(com -> coms.forEach(e -> e.addPeer(com)));

        System.out.println("Initial state:\n"
                + coms.stream().map(e -> e.getProtocol().getCurrent()).collect(Collectors.toList()));

        @SuppressWarnings("unchecked")
        List<CompletableFuture<Color>> promises = coms.stream()
                                                      .filter(com -> com.getProtocol().getCurrent() != null)
                                                      .map(com -> com.getProtocol().loop())
                                                      .map(e -> (CompletableFuture<Color>) e) // Silly compiler issue
                                                      .collect(Collectors.toList());

        CompletableFuture<Color> promise = promises.get(0);
        assertNotNull(promise);
        promise.get();

        List<Object> results = coms.stream().map(com -> {
            try {
                return com.getProtocol().finalizeChoice();
            } catch (Throwable t) {
                return t.getMessage();
            }
        }).collect(Collectors.toList());

        List<Color> nonErrors = results.stream()
                                       .filter(e -> e instanceof Color)
                                       .map(e -> (Color) e)
                                       .collect(Collectors.toList());
        System.out.println("Results:\n" + results);

        assertEquals(cardinality, nonErrors.size(), "Errors in querying");

        Color chosen = nonErrors.get(0);
        List<Color> different = nonErrors.stream().filter(c -> c != chosen).collect(Collectors.toList());
        assertEquals(0, different.size(), "Not everyone agrees!");

    }

    @Test
    public void smoke() {
        int cardinality = 10;

        Random entropy = new Random(0x666);
        List<MockCommunications> coms = new ArrayList<>();
        for (int port = 0; port < cardinality; port++) {
            Color initialColor = entropy.nextBoolean() ? null : entropy.nextBoolean() ? Color.Blue : Color.Red;
            coms.add(newMember(port, initialColor, cardinality));
        }
        coms.forEach(com -> coms.forEach(e -> e.addPeer(com)));

        System.out.println("Initial state:\n"
                + coms.stream().map(e -> e.getProtocol().getCurrent()).collect(Collectors.toList()));

        @SuppressWarnings("unchecked")
        List<CompletableFuture<Color>> promises = coms.stream()
                                                      .map(com -> com.getProtocol().loop())
                                                      .map(e -> (CompletableFuture<Color>) e) // Silly compiler issue
                                                      .collect(Collectors.toList());
        assertEquals(cardinality, promises.size());
        List<Color> results = promises.stream().map(promise -> {
            try {
                return promise.get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }).filter(result -> result != null).collect(Collectors.toList());
        assertEquals(cardinality, results.size());

        System.out.println("Results:\n" + results);

        Color chosen = results.get(0);
        List<Color> different = results.stream().filter(c -> c != chosen).collect(Collectors.toList());
        assertEquals(0, different.size(), "Not everyone agrees!");

    }

    protected abstract MockCommunications newMember(int port, Color initialColor, int cardinality);

}
