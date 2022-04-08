/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.ethereal.random.beacon.Beacon;
import com.salesforce.apollo.ethereal.random.beacon.DeterministicRandomSource.DsrFactory;
import com.salesforce.apollo.ethereal.random.coin.Coin;
import com.salesforce.apollo.utils.Utils;

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
public class Ethereal {

    public record Controller(Runnable starte, Runnable stope, Orderer orderer) {
        public void start() {
            starte.run();
        }

        public void stop() {
            stope.run();
        }
    }

    public record PreBlock(List<ByteString> data, byte[] randomBytes) {}

    private static final Logger log = LoggerFactory.getLogger(Ethereal.class);;

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
     * Processing entry point for Ethereal using an asynchronous, BFT random beacon.
     * This provides strong liveness guarantees for the instance.
     * 
     * Given two Config objects (one for the setup phase and one for the main
     * consensus), data source and preblock sink, initialize two orderers and a
     * channel between them used to pass the result of the setup phase. The first
     * phase is the setup phase for the instance, and generates the weak threshold
     * key for subsequent randomness, to be revealed when appropriate in the
     * protocol phases.
     * 
     * @param setupConfig    - configuration for random beacon setup phase
     * @param config         - the Config
     * @param ds             - the DataSource to use to build Units
     * @param onClose        - run when the consensus has produced all units for
     *                       epochs
     * @param prebblockSink  - the channel to send assembled PreBlocks as output
     * @param roundScheduler
     * @param newEpochAction
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller abftRandomBeacon(Config setupConfig, Config config, DataSource ds,
                                       Consumer<PreBlock> preblockSink, Runnable onClose,
                                       Consumer<Integer> newEpochAction) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        this.config = config;
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        Controller setup = setup(setupConfig, wtkChan);
        Controller consensus = consensus(config, wtkChan, ds, preblockSink, onClose, newEpochAction);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return new Controller(() -> {
            setup.starte.run();
            consensus.starte.run();
        }, () -> {
            setup.stope.run();
            consensus.stope.run();
        }, null);
    }

    /**
     * Deterministic consensus processing entry point for Ethereal. Does not
     * strongly guarantee liveness, but does guarantee correctness.
     * 
     * @param config         - the Config
     * @param ds             - the DataSource to use to build Units
     * @param prebblockSink  - the channel to send assembled PreBlocks as output
     * @param roundScheduler
     * @param newEpochAction
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller deterministic(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                    Consumer<Integer> newEpochAction) {
        if (started.get()) {
            return null;
        }
        this.config = config;
        Controller consensus = deterministicConsensus(config, ds, blocker, newEpochAction);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
//        System.out.println("Controller config: " + config);
        return new Controller(consensus.starte, consensus.stope, consensus.orderer);
    }

    public Config getConfig() {
        return config;
    }

    /**
     * A counterpart of consensus() in that this does not perform the setup phase
     * for the ABFT random beacon, rather relying on a fixed seeded WeakThresholdKey
     * is used for the main consensus.
     * 
     * @param conf           - the Config
     * @param ds             - the DataSource to use to build Units
     * @param onClose        - run when the consensus has produced all units for
     *                       epochs
     * @param prebblockSink  - the channel to send assembled PreBlocks as output
     * @param roundScheduler
     * @param connector      - the Consumer of the created Orderer for this system
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller weakBeacon(Config conf, DataSource ds, Consumer<PreBlock> preblockSink, Runnable onClose) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        this.config = conf;
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        var consensus = consensus(conf, wtkChan, ds, preblockSink, onClose, null);
        return new Controller(() -> {
            consensus.starte.run();
            try {
                wtkChan.exchange(WeakThresholdKey.seededWTK(conf.nProc(), conf.pid(), 2137, null));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }, consensus.starte, null);
    }

    private Controller consensus(Config config, Exchanger<WeakThresholdKey> wtkChan, DataSource ds,
                                 Consumer<PreBlock> preblockSink, Runnable onClose, Consumer<Integer> newEpochAction) {
        Consumer<List<Unit>> makePreblock = units -> {
            PreBlock preBlock = toPreBlock(units);
            if (preBlock != null) {
                log.debug("Emitting pre block on: {}", config.logLabel());
                preblockSink.accept(preBlock);
            }
            var timingUnit = units.get(units.size() - 1);
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                // we have just sent the last preblock of the last epoch, it's safe to quit
                if (onClose != null) {
                    Utils.wrapped(onClose, log).run();
                }
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();
        var started = new AtomicBoolean();
        Runnable start = () -> {
            try {
                WeakThresholdKey wtkey;
                try {
                    wtkey = wtkChan.exchange(null);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Unable to exchange wtk on: " + config.logLabel(), e);
                }
                logWTK(wtkey);
                var orderer = new Orderer(Config.builderFrom(config).setWtk(wtkey).build(), ds, makePreblock,
                                          newEpochAction, Coin.newFactory(config.pid(), wtkey));
                ord.set(orderer);
                orderer.start();
            } finally {
                started.set(true);
            }
        };
        Runnable stop = () -> {
            if (!Utils.waitForCondition(10, () -> started.get())) {
                log.trace("Waited 10ms for start and unsuccessful, proceeding on: " + config.logLabel());
            }
            Orderer orderer = ord.get();
            if (orderer != null) {
                orderer.stop();
            }
        };
        return new Controller(start, stop, null);
    }

    private Controller deterministicConsensus(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                              Consumer<Integer> newEpochAction) {
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

        var orderer = new Orderer(config, ds, makePreblock, newEpochAction, new DsrFactory(config.digestAlgorithm()));

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

    private void logWTK(WeakThresholdKey wtkey) {
        var providers = new ArrayList<Short>();
        for (var provider : wtkey.shareProviders().keySet()) {
            providers.add(provider);
        }
        log.info("Global Weak Threshold Key threshold: {} share providers: {} on: {}", wtkey.threshold(), providers,
                 config.logLabel());
    }

    private Controller setup(Config conf, Exchanger<WeakThresholdKey> wtkChan) {
        var rsf = new Beacon(conf);
        Consumer<List<Unit>> extractHead = units -> {
            var head = units.get(units.size() - 1);
            if (head.level() == conf.orderStartLevel()) {
                try {
                    wtkChan.exchange(rsf.getWTK(head.creator()));
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Error publishing weak threshold key downstream on: "
                    + conf.logLabel(), e);
                }
            } else {
                throw new IllegalStateException("Setup phase: wrong level on: " + conf.logLabel());
            }
        };

        var ord = new Orderer(conf, null, extractHead, null, rsf);
        return new Controller(() -> ord.start(), () -> ord.stop(), null);
    }
}
