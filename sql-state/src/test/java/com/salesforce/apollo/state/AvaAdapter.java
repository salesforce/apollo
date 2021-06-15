/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.Avalanche.Finalized;
import com.salesforce.apollo.avalanche.Processor;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class AvaAdapter implements Processor {
    private Avalanche                             avalanche;
    private Consortium                            consortium;
    private final AtomicReference<CountDownLatch> processed;

    public AvaAdapter(AtomicReference<CountDownLatch> processed) {
        this.processed = processed;
    }

    @Override
    public void finalize(FinalizationData finalized) {
        finalized.finalized.stream()
                           .map(f -> certifiedBlock(f))
                           .filter(cb -> cb != null)
                           .peek(cb -> consortium.process(cb))
                           .forEach(cb -> processed.get().countDown());
    }

    public Avalanche getAvalanche() {
        return avalanche;
    }

    public BiFunction<CertifiedBlock, CompletableFuture<?>, Digest> getConsensus() {
        return (cb, f) -> avalanche.submitTransaction(cb, new Digest(cb.getBlock().getHeader().getPrevious()));
    }

    public Consortium getConsortium() {
        return consortium;
    }

    public void setAva(Avalanche avalanche) {
        this.avalanche = avalanche;
    }

    public void setAvalanche(Avalanche avalanche) {
        this.avalanche = avalanche;
    }

    public void setConsortium(Consortium consortium) {
        this.consortium = consortium;
    }

    @Override
    public Digest validate(Digest key, DagEntry entry) {
        CertifiedBlock cb = certifiedBlock(entry);
        if (cb == null) {
            System.out.println("null from: " + key + " descr: " + entry.getDescription());
            return key;
        }
        return new Digest(cb.getBlock().getHeader().getPrevious());
    }

    private CertifiedBlock certifiedBlock(DagEntry entry) {
        try {
            if (entry.getData().is(CertifiedBlock.class)) {
                return entry.getData().unpack(CertifiedBlock.class);
            }
        } catch (InvalidProtocolBufferException e) {
        }
        return null;
    }

    private CertifiedBlock certifiedBlock(Finalized f) {
        DagEntry entry = f.entry;
        return certifiedBlock(entry);
    }
}
