/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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

    public record Controller(Runnable starte, Runnable stope, BiConsumer<Short, List<PreUnit>> input, Runnable sync) {
        public void start() {
            starte.run();
        }

        public void stop() {
            stope.run();
        }

        public void synchronize() {
            sync.run();
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
     * @param onClose       - run when the consensus has produced all units for
     *                      epochs
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller abftRandomBeacon(Config setupConfig, Config config, DataSource ds,
                                       Consumer<PreBlock> preblockSink, Consumer<PreUnit> synchronizer,
                                       Runnable onClose) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        Controller setup = setup(setupConfig, wtkChan);
        Controller consensus = consensus(config, wtkChan, ds, preblockSink, synchronizer, onClose);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return new Controller(() -> {
            setup.starte.run();
            consensus.starte.run();
        }, () -> {
            setup.stope.run();
            consensus.stope.run();
        }, consensus.input, consensus.sync);
    }

    /**
     * Deterministic consensus processing entry point for Ethereal. Does not
     * strongly guarantee liveness, but does guarantee correctness.
     * 
     * @param config        - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller deterministic(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                    Consumer<PreUnit> synchronizer) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Controller consensus = deterministicConsensus(config, ds, blocker, synchronizer);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return new Controller(consensus.starte, consensus.stope, consensus.input, consensus.sync);
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
     * @param onClose       - run when the consensus has produced all units for
     *                      epochs
     * @param connector     - the Consumer of the created Orderer for this system
     * @return the Controller for starting/stopping this instance, or NULL if
     *         already started.
     */
    public Controller weakBeacon(Config conf, DataSource ds, Consumer<PreBlock> preblockSink,
                                 Consumer<PreUnit> synchronizer, Runnable onClose) {
        if (!started.compareAndSet(false, true)) {
            return null;
        }
        Exchanger<WeakThresholdKey> wtkChan = new Exchanger<>();
        var consensus = consensus(conf, wtkChan, ds, preblockSink, synchronizer, onClose);
        return new Controller(() -> {
            consensus.starte.run();
            try {
                wtkChan.exchange(WeakThresholdKey.seededWTK(conf.nProc(), conf.pid(), 2137, null));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }, consensus.starte, consensus.input, consensus.sync);
    }

    private Controller consensus(Config config, Exchanger<WeakThresholdKey> wtkChan, DataSource ds,
                                 Consumer<PreBlock> preblockSink, Consumer<PreUnit> synchronizer, Runnable onClose) {
        Consumer<List<Unit>> makePreblock = units -> {
            PreBlock preBlock = toPreBlock(units);
            if (preBlock != null) {
                log.debug("Emitting pre block: {} on: {}");
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
        BiConsumer<Short, List<PreUnit>> input = (source, pus) -> ord.get().addPreunits(source, pus);
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
                } finally {
                    started.set(true);
                }
            });
        };
        Runnable stop = () -> {
            if (!Utils.waitForCondition(10, () -> started.get())) {
                log.trace("Waited 10ms for start and unsuccessful, proceeding");
            }
            Orderer orderer = ord.get();
            if (orderer != null) {
                orderer.stop();
            }
        };
        return new Controller(start, stop, input, () -> ord.get().sync(synchronizer));
    }

    private Controller deterministicConsensus(Config config, DataSource ds, BiConsumer<PreBlock, Boolean> blocker,
                                              Consumer<PreUnit> synchronizer) {
        Consumer<List<Unit>> makePreblock = units -> {
            log.trace("Make pre block: {} on: {}", units, config.pid());
            PreBlock preBlock = toPreBlock(units);
            var timingUnit = units.get(units.size() - 1);
            var last = false;
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                log.info("Closing at last level: {} and epochs: {} on: {}", timingUnit.level(), timingUnit.epoch(),
                         config.pid());
                last = true;
            }
            if (preBlock != null) {
                log.debug("Emitting pre block: {} on: {}", units, config.pid());
                try {
                    blocker.accept(preBlock, last);
                } catch (Throwable t) {
                    log.error("Error consuming pre block: {} on: {}", units, config.pid(), t);
                }
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();

        var in = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "Input for: " + config.pid());
            t.setDaemon(true);
            return t;
        });
        BiConsumer<Short, List<PreUnit>> input = (source, pus) -> {
            try {
                in.execute(() -> {
                    Orderer orderer = ord.get();
                    if (orderer != null) {
                        orderer.addPreunits(source, pus);
                    }
                });
            } catch (RejectedExecutionException e) {
                // ignored
            }
        };

        Runnable start = () -> {
            var orderer = new Orderer(config, ds, makePreblock, Clock.systemUTC());
            ord.set(orderer);
            orderer.start(new DsrFactory(), synchronizer);
        };
        Runnable stop = () -> {
            in.shutdown();
            Orderer orderer = ord.get();
            if (orderer != null) {
                orderer.stop();
            }
        };
        return new Controller(start, stop, input, () -> ord.get().sync(synchronizer));
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

        var ord = new Orderer(conf, null, extractHead, Clock.systemUTC());
        return new Controller(() -> ord.start(rsf, p -> {
        }), () -> ord.stop(), null, null);
    }
}
