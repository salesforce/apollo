/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal.rbc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.random.beacon.DeterministicRandomSource.DsrFactory;

/**
 * The Ethereal instance represents a list of linearly decided batches of
 * transactions, refered to as <code>Unit</code>s. These Units are causally
 * arranged in a linear order.
 * 
 * Ethereal has three ways to invoke consensus upon its members, each providing
 * different guarantees.
 * 
 * The strongest guarantees are provided by the <code>abftRandomBeacon</code>()
 * method, which provides correct and live guarantees.
 * 
 * The remaining 2 methods, <code>weakBeacon</code>() and
 * <code>deterministic</code>() provide equal guarantees for correctness,
 * however neither guarantee liveness. The liveness guarantees provided are
 * quite strong and difficult to actually subvert, however, despite not being
 * provably air tight.
 * 
 * @author hal.hildebrand
 *
 */
public class ChRbcEthereal {

    public record Controller(Runnable starte, Runnable stope, ChRbcOrderer orderer) {
        public void start() {
            starte.run();
        }

        public void stop() {
            stope.run();
        }
    }

    public record PreBlock(List<ByteString> data, byte[] randomBytes) {}

    private static final Logger log = LoggerFactory.getLogger(ChRbcEthereal.class);;

    /**
     * return a preblock from a slice of units containing a timing round. It assumes
     * that the timing unit is the last unit in the slice, and that random source
     * data of the timing unit starts with random bytes from the previous level.
     */
    public static PreBlock toPreBlock(List<Unit> round) {
        var data = new ArrayList<ByteString>();
        for (Unit u : round) {
            if (!u.dealing()) {// data in dealing units doesn't come from users, these are new epoch proofs
                data.add(u.data());
            }
        }
        var randomBytes = round.get(round.size() - 1).randomSourceData();
        return data.isEmpty() ? null : new PreBlock(data, randomBytes);
    }

    private Config              config;
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * Deterministic consensus processing entry point for Ethereal. Does not
     * strongly guarantee liveness, but does guarantee correctness.
     * 
     * @param config         - the Config
     * @param ds             - the DataSource to use to build Units
     * @param newEpochAction
     * @param gossiper
     * @param prebblockSink  - the channel to send assembled PreBlocks as output
     * @param roundScheduler
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller deterministic(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                    Consumer<Integer> newEpochAction, Consumer<Processor> gossiper) {
        if (started.get()) {
            return null;
        }
        this.config = config;
        Controller consensus = deterministicConsensus(config, ds, blocker, newEpochAction, gossiper);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
//        System.out.println("Controller config: " + config);
        return consensus;
    }

    public Config getConfig() {
        return config;
    }

    private Controller deterministicConsensus(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                              Consumer<Integer> newEpochAction, Consumer<Processor> gossiper) {
        Consumer<List<Unit>> makePreblock = units -> {
            log.trace("Make pre block: {} on: {}", units, config.logLabel());
            PreBlock preBlock = toPreBlock(units);
            var timingUnit = units.get(units.size() - 1);
            var last = false;
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                log.debug("Closing at last level: {} at epoch: {} on: {}", timingUnit.level(), timingUnit.epoch(),
                          config.logLabel());
                last = true;
            }
            if (preBlock != null) {
                log.debug("Emitting pre block: {} on: {}", units, config.logLabel());
                try {
                    blocker.accept(preBlock, last);
                } catch (Throwable t) {
                    log.error("Error consuming pre block: {} on: {}", units, config.logLabel(), t);
                }
            }
        };

        var orderer = new ChRbcOrderer(config, 0, ds, makePreblock, newEpochAction,
                                       new DsrFactory(config.digestAlgorithm()), gossiper);

        Runnable start = () -> {
            log.debug("Starting Ethereal on: {}", config.logLabel());
            started.set(true);
            orderer.start();
        };
        Runnable stop = () -> {
            log.debug("Stopping Ethereal on: {}", config.logLabel());
            started.set(false);
            orderer.stop();
        };
        return new Controller(start, stop, orderer);
    }
}
