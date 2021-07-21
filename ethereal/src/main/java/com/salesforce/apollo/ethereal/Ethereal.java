/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.security.PublicKey;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.random.beacon.Beacon;
import com.salesforce.apollo.ethereal.random.beacon.DeterministicRandomSource.DsrFactory;
import com.salesforce.apollo.ethereal.random.coin.Coin;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Ethereal {
    public record Controller(Runnable start, Runnable stop) {};

    private static final Logger             log       = LoggerFactory.getLogger(Ethereal.class);
    private final Map<String, List<String>> addresses = new HashMap<>();
    private final Context<Member>           context;
    // members assigned a process id, 0 -> Context cardinality. Determined by id
    // (hash) sort order
    private final PublicKey[]              p2pKeys;
    private final SortedMap<Digest, Short> pids;
    private final PublicKey[]              publicKeys;

    private final Map<String, List<String>> setupAddresses = new HashMap<>();

    public Ethereal(Context<Member> context) {
        this.context = context;
        AtomicInteger i = new AtomicInteger(0);
        // Organize the members of the context by labeling them with short integer
        // values. The pids maps members to this id, and the map is sorted and ordered
        // by member id, with key index == member's pid
        pids = context.allMembers().sorted()
                      .collect(Collectors.toMap(m -> m.getId(), m -> (short) i.getAndIncrement(), (a, b) -> {
                          throw new IllegalStateException();
                      }, () -> new TreeMap<>()));
        publicKeys = new PublicKey[pids.size()];
        p2pKeys = new PublicKey[pids.size()];
    }

    /**
     * The main processing entry point for Ethereal. Given two Config objects (one
     * for the setup phase and one for the main consensus), data source and preblock
     * sink, initialize two orderers and a channel between them used to pass the
     * result of the setup phase. Returns two functions that can be used to,
     * respectively, start and stop the whole system. The provided preblock sink
     * gets closed after producing the last preblock.
     * 
     * @param conf          - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     */
    public Controller deterministic(Config setupConfig, Config config, DataSource ds,
                                    SimpleChannel<PreBlock> preblockSink, SimpleChannel<PreUnit> synchronizer,
                                    Consumer<Orderer> connector) {
        Controller consensus = deterministicConsensus(config, ds, preblockSink, synchronizer, connector);
        if (consensus == null) {
            throw new IllegalStateException("Error occurred initializing consensus");
        }
        return consensus;
    }

    /**
     * A counterpart of process() that does not perform the setup phase. Instead, a
     * fixed seeded WeakThresholdKey is used for the main consensus. NoBeacon should
     * be used for testing purposes only! Returns start and stop functions.
     * 
     * @param conf          - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     */
    public Controller noBeacon(Config conf, DataSource ds, SimpleChannel<PreBlock> preblockSink,
                               SimpleChannel<PreUnit> synchronizer, Consumer<Orderer> connector) {
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

    /**
     * Deterministic consensus processing entry point for Ethereal. Returns two
     * functions that can be used to, respectively, start and stop the whole system.
     * The provided preblock sink gets closed after producing the last preblock.
     * 
     * @param conf          - the Config
     * @param ds            - the DataSource to use to build Units
     * @param prebblockSink - the channel to send assembled PreBlocks as output
     * @param synchronizer  - the channel that broadcasts PreUnits to other members
     * @param connector     - the Consumer of the created Orderer for this system
     */
    public Controller process(Config setupConfig, Config config, DataSource ds, SimpleChannel<PreBlock> preblockSink,
                              SimpleChannel<PreUnit> synchronizer, Consumer<Orderer> connector) {
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

    private Controller consensus(Config config, Exchanger<WeakThresholdKey> wtkChan, DataSource ds,
                                 SimpleChannel<PreBlock> preblockSink, SimpleChannel<PreUnit> synchronizer,
                                 Consumer<Orderer> connector) {
        Consumer<List<Unit>> makePreblock = units -> {
            preblockSink.submit(Data.toPreBlock(units));
            var timingUnit = units.get(units.size() - 1);
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                // we have just sent the last preblock of the last epoch, it's safe to quit
                preblockSink.close();
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();

        var started = new BlockingArrayQueue<>(1);
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
                    started.add(true);
                }
            });
        };
        Runnable stop = () -> {
            started.poll();
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
            preblockSink.submit(Data.toPreBlock(units));
            var timingUnit = units.get(units.size() - 1);
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                // we have just sent the last preblock of the last epoch, it's safe to quit
                preblockSink.close();
            }
        };

        AtomicReference<Orderer> ord = new AtomicReference<>();

        var started = new BlockingArrayQueue<>(1);
        Runnable start = () -> {
            config.executor().execute(() -> {
                try {
                    var orderer = new Orderer(Config.builderFrom(config).build(), ds, makePreblock, Clock.systemUTC());
                    ord.set(orderer);
                    orderer.start(new DsrFactory(), synchronizer);
                    connector.accept(orderer);
                } finally {
                    started.add(true);
                }
            });
        };
        Runnable stop = () -> {
            started.poll();
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
        if (rsf != null) {
            return null;
        }

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
