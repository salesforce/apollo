/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.noGaps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
import com.salesforce.apollo.consortium.support.ProcessedBuffer;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */

public class Regency {
    private final AtomicInteger currentRegent = new AtomicInteger(0);

    private final Map<Integer, Map<Member, StopData>> data              = new ConcurrentHashMap<>();
    private final Logger                              log               = LoggerFactory.getLogger(Regency.class);
    private final AtomicInteger                       nextRegent        = new AtomicInteger(-1);
    private final Set<EnqueuedTransaction>            stopMessages      = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<Integer, Sync>                  sync              = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Member>>           wantRegencyChange = new ConcurrentHashMap<>();

    public int currentRegent() {
        return currentRegent.get();
    }

    public void currentRegent(int c) {
        int prev = currentRegent.getAndSet(c);
        if (prev != c) {
            assert c == prev || c > prev || c == -1 : "whoops: " + prev + " -> " + c;
            log.trace("Current regency set to: {} previous: {}", c, prev);
        }
    }

    public void deliverStop(Stop data, Member from, Consortium consortium, View view,
                            Map<HashKey, EnqueuedTransaction> toOrder, ProcessedBuffer processed) {
        if (sync.containsKey(data.getNextRegent())) {
            log.trace("Ignoring stop, already sync'd: {} from {} on: {}", data.getNextRegent(), from,
                      consortium.getMember());
            return;
        }
        Set<Member> votes = wantRegencyChange.computeIfAbsent(data.getNextRegent(),
                                                              k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

        if (votes.size() >= view.getContext().majority()) {
            log.trace("Ignoring stop, already established: {} from {} on: {} requesting: {}", data.getNextRegent(),
                      from, consortium.getMember(), votes.stream().map(m -> m.getId()).collect(Collectors.toList()));
            return;
        }
        log.debug("Delivering stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(), currentRegent(),
                  votes.size(), from, consortium.getMember());
        votes.add(from);
        data.getTransactionsList()
            .stream()
            .map(tx -> new EnqueuedTransaction(Consortium.hashOf(tx), tx))
            .peek(eqt -> stopMessages.add(eqt))
            .filter(eqt -> !processed.contains(eqt.hash))
            .forEach(eqt -> {
                toOrder.putIfAbsent(eqt.hash, eqt);
            });
        if (votes.size() >= view.getContext().majority()) {
            log.debug("Majority acheived, stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(),
                      currentRegent(), votes.size(), from, consortium.getMember());
            List<EnqueuedTransaction> msgs = stopMessages.stream().collect(Collectors.toList());
            stopMessages.clear();
            consortium.transitions.startRegencyChange(msgs);
            consortium.transitions.stopped();
        } else {
            log.debug("Majority not acheived, stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(),
                      currentRegent(), votes.size(), from, consortium.getMember());
        }
    }

    public void deliverStopData(StopData stopData, Member from, View view, Consortium consortium) {
        int elected = stopData.getCurrentRegent();
        Map<Member, StopData> regencyData = data.computeIfAbsent(elected,
                                                                 k -> new ConcurrentHashMap<Member, StopData>());
        int majority = view.getContext().majority();

        Member regent = view.getContext().getRegent(elected);
        if (!consortium.getMember().equals(regent)) {
            log.trace("ignoring StopData from {} on: {}, incorrect regent: {} not this member: {}", from,
                      consortium.getMember(), elected, regent);
            return;
        }
        if (nextRegent() != elected) {
            log.trace("ignoring StopData from {} on: {}, incorrect regent: {} not next regent: {})", from,
                      consortium.getMember(), elected, nextRegent());
            return;
        }
        if (sync.containsKey(elected)) {
            log.trace("ignoring StopData from {} on: {}, already synchronized for regency: {}", from,
                      consortium.getMember(), elected);
            return;
        }

        Map<HashKey, CertifiedBlock> hashed;
        List<HashKey> hashes = new ArrayList<>();
        hashed = stopData.getBlocksList().stream().collect(Collectors.toMap(cb -> {
            HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
            hashes.add(hash);
            return hash;
        }, cb -> cb));
        List<Long> gaps = noGaps(hashed, consortium.store.hashes());
        if (!gaps.isEmpty()) {
            log.trace("ignoring StopData: {} from {} on: {} gaps in log: {}", elected, from, consortium.getMember(),
                      gaps);
            return;
        }

        log.debug("Delivering StopData: {} from {} on: {}", elected, from, consortium.getMember());
        regencyData.put(from, stopData);
        if (regencyData.size() >= majority) {
            consortium.transitions.synchronize(elected, regencyData);
        } else {
            log.trace("accepted StopData: {} votes: {} from {} on: {}", elected, regencyData.size(), from,
                      consortium.getMember());
        }
    }

    public void deliverSync(Sync syncData, Member from, View view, CollaboratorContext cContext) {
        int cReg = syncData.getCurrentRegent();
        Member regent = view.getContext().getRegent(cReg);
        if (sync.get(cReg) != null) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} consensus already proved", from, cReg,
                      cContext.consortium.getMember());
            return;
        }

        if (!validate(from, syncData, cReg, view, cContext.consortium)) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log", from, cReg,
                      cContext.consortium.getMember());
            return;
        }
        if (!validate(regent, syncData, cReg, view, cContext.consortium)) {
            log.error("Invalid Sync from: {} regent: {} current: {} on: {}", from, cReg, currentRegent(),
                      cContext.consortium.getMember());
            return;
        }
        log.debug("Delivering Sync from: {} regent: {} current: {} on: {}", from, cReg, currentRegent(),
                  cContext.consortium.getMember());
        currentRegent(nextRegent());
        sync.put(cReg, syncData);
        cContext.synchronize(syncData, regent);
        cContext.consortium.transitions.syncd();
        resolveRegentStatus(cContext.consortium, view);
    }

    public int nextRegent() {
        return nextRegent.get();
    }

    public void nextRegent(int n) {
        nextRegent.set(n);
    }

    private void resolveRegentStatus(Consortium consortium, View view) {
        Member regent = view.getContext().getRegent(nextRegent());
        log.debug("Regent: {} on: {}", regent, consortium.getMember());
        if (consortium.getMember().equals(regent)) {
            consortium.transitions.becomeLeader();
        } else {
            consortium.transitions.becomeFollower();
        }
    }

    private boolean validate(Member regent, Sync sync, int regency, View view, Consortium consortium) {
        Map<HashKey, CertifiedBlock> hashed;
        List<HashKey> hashes = new ArrayList<>();
        hashed = sync.getBlocksList()
                     .stream()
                     .filter(cb -> view.getContext().validate(cb))
                     .collect(Collectors.toMap(cb -> {
                         HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                         hashes.add(hash);
                         return hash;
                     }, cb -> cb));
        List<Long> gaps = noGaps(hashed, consortium.store.hashes());
        if (!gaps.isEmpty()) {
            log.debug("Rejecting Sync from: {} regent: {} on: {} gaps in Sync log: {}", regent, regency,
                      consortium.getMember(), gaps);
            return false;
        }
        return true;
    }
}
