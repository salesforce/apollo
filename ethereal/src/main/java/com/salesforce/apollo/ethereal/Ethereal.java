/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.random.beacon.Beacon;
import com.salesforce.apollo.ethereal.random.beacon.DeterministicRandomSource.DsrFactory;
import com.salesforce.apollo.ethereal.random.coin.Coin;
import com.salesforce.apollo.utils.SimpleChannel;
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
    public interface Committee {
        class Default implements Committee {
            private final Map<Short, Verifier> verifiers;

            public Default(Map<Short, Verifier> verifiers) {
                this.verifiers = new HashMap<>(verifiers);
            }

            @Override
            public Verifier getVerifier(short pid) {
                return verifiers.get(pid);
            }
        }

        Verifier getVerifier(short pid);
    }

    public record Controller(Runnable start, Runnable stop) {};

    private static final Logger log     = LoggerFactory.getLogger(Ethereal.class);
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
     * @param setupConfig   - configuration for random beacon setup phase
     * @param config        - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller abftRandomBeacon(Config setupConfig, Config config, DataSource ds,
                                       SimpleChannel<PreBlock> preblockSink, SimpleChannel<PreUnit> synchronizer,
                                       Consumer<Orderer> connector) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        Controller setup = setup(setupConfig, wtkChan);
        Controller consensus = consensus(config, wtkChan, ds, preblockSink, synchronizer, connector);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return new Controller(() -> {
            setup.start.run();
            consensus.start.run();
        }, () -> {
            setup.stop.run();
            consensus.stop.run();
        });
    }

    /**
     * Deterministic consensus processing entry point for Ethereal. Does not
     * strongly guarantee liveness, but does guarantee correctness.
     * 
     * @param config        - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller deterministic(Config config, DataSource ds, SimpleChannel<PreBlock> preblockSink,
                                    SimpleChannel<PreUnit> synchronizer, Consumer<Orderer> connector) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Controller consensus = deterministicConsensus(config, ds, preblockSink, synchronizer, connector);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return new Controller(() -> config.executor().execute(consensus.start),
                              () -> config.executor().execute(consensus.stop));
    }

    /**
     * A counterpart of consensus() in that this does not perform the setup phase
     * for the ABFT random beacon, rather relying on a fixed seeded WeakThresholdKey
     * is used for the main consensus.
     * 
     * @param conf          - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller weakBeacon(Config conf, DataSource ds, SimpleChannel<PreBlock> preblockSink,
                                 SimpleChannel<PreUnit> synchronizer, Consumer<Orderer> connector) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        var consensus = consensus(conf, wtkChan, ds, preblockSink, synchronizer, connector);
        return new Controller(() -> {
            consensus.start.run();
            try {
                wtkChan.exchange(WeakThresholdKey.seededWTK(conf.nProc(), conf.pid(), 2137, null));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }, consensus.stop);
    }

    private Controller consensus(Config config, Exchanger<WeakThresholdKey> wtkChan, DataSource ds,
                                 SimpleChannel<PreBlock> preblockSink, SimpleChannel<PreUnit> synchronizer,
                                 Consumer<Orderer> connector) {
        Consumer<List<Unit>> makePreblock = units -> {
            PreBlock preBlock = Data.toPreBlock(units);
            if (preBlock != null) {
                preblockSink.submit(preBlock);
            }
            var timingUnit = units.get(units.size() - 1);
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                // we have just sent the last preblock of the last epoch, it's safe to quit
                preblockSink.close();
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();

        var started = new AtomicBoolean();
        Runnable start = () -> {
            config.executor().execute(() -> {
                try {
                    WeakThresholdKey wtkey;
                    try {
                        wtkey = wtkChan.exchange(null);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException("Unable to exchange wtk", e);
                    }
                    logWTK(wtkey);
                    var orderer = new Orderer(Config.builderFrom(config).setWtk(wtkey).build(), ds, makePreblock,
                                              Clock.systemUTC());
                    ord.set(orderer);
                    orderer.start(Coin.newFactory(config.pid(), wtkey), synchronizer);
                    connector.accept(orderer);
                } finally {
                    started.set(true);
                }
            });
        };
        Runnable stop = () -> {
            if (!Utils.waitForCondition(2_000, () -> started.get())) {
                log.error("Waited 2 seconds for start and unsuccessful, proceeding");
            }
            Orderer orderer = ord.get();
            if (orderer != null) {
                orderer.stop();
            }
        };
        return new Controller(start, stop);
    }

    private Controller deterministicConsensus(Config config, DataSource ds, SimpleChannel<PreBlock> preblockSink,
                                              SimpleChannel<PreUnit> synchronizer, Consumer<Orderer> connector) {
        Consumer<List<Unit>> makePreblock = units -> {
            PreBlock preBlock = Data.toPreBlock(units);
            if (preBlock != null) {
                preblockSink.submit(preBlock);
            }
            var timingUnit = units.get(units.size() - 1);
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                log.info("Closing at last level: {} and epochs: {}", timingUnit.level(), timingUnit.epoch());
                // we have just sent the last preblock of the last epoch, it's safe to quit
                preblockSink.close();
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();

        AtomicBoolean started = new AtomicBoolean();
        Runnable start = () -> {
            config.executor().execute(() -> {
                try {
                    var orderer = new Orderer(Config.builderFrom(config).build(), ds, makePreblock, Clock.systemUTC());
                    ord.set(orderer);
                    orderer.start(new DsrFactory(), synchronizer);
                    connector.accept(orderer);
                } finally {
                    started.set(true);
                }
            });
        };
        Runnable stop = () -> {
            if (!Utils.waitForCondition(2_000, () -> started.get())) {
                log.error("Waited 2 seconds for start and unsuccessful, proceeding");
            }
            Orderer orderer = ord.get();
            if (orderer != null) {
                orderer.stop();
            }
        };
        return new Controller(start, stop);
    }

    private void logWTK(WeakThresholdKey wtkey) {
        var providers = new ArrayList<Short>();
        for (var provider : wtkey.shareProviders().keySet()) {
            providers.add(provider);
        }
        log.info("Global Weak Threshold Key threshold: {} share providers: {}", wtkey.threshold(), providers);
    }

    private Controller setup(Config conf, Exchanger<WeakThresholdKey> wtkChan) {
        var rsf = new Beacon(conf);
        Consumer<List<Unit>> extractHead = units -> {
            var head = units.get(units.size() - 1);
            if (head.level() == conf.orderStartLevel()) {
                try {
                    wtkChan.exchange(rsf.getWTK(head.creator()));
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Error publishing weak threshold key downstream", e);
                }
            } else {
                throw new IllegalStateException("Setup phase: wrong level");
            }
        };

        SimpleChannel<PreUnit> syn = new SimpleChannel<>(1000);
        var ord = new Orderer(conf, null, extractHead, Clock.systemUTC());
        return new Controller(() -> ord.start(rsf, syn), () -> ord.stop());
    }
}
