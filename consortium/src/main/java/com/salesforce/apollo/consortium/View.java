/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.support.SigningUtils.generateKeyPair;
import static com.salesforce.apollo.consortium.support.SigningUtils.sign;

import java.security.KeyPair;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.comms.LinearClient;
import com.salesforce.apollo.consortium.comms.LinearServer;
import com.salesforce.apollo.consortium.support.TickScheduler;
import com.salesforce.apollo.membership.messaging.MemberOrder;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class View {
    private static final Logger log = LoggerFactory.getLogger(View.class);

    private final AtomicReference<CommonCommunications<LinearClient, Service>>   comm                     = new AtomicReference<>();
    private final AtomicReference<ViewContext>                                   context                  = new AtomicReference<>();
    private final Function<HashKey, CommonCommunications<LinearClient, Service>> createClientComms;
    private final AtomicReference<Messenger>                                     messenger                = new AtomicReference<>();
    private final AtomicReference<ViewMember>                                    nextView                 = new AtomicReference<>();
    private final AtomicReference<KeyPair>                                       nextViewConsensusKeyPair = new AtomicReference<>();
    private final AtomicReference<MemberOrder>                                   order                    = new AtomicReference<>();
    private final Parameters                                                     params;
    private final BiConsumer<HashKey, List<Msg>>                                 process;

    public View(Service service, Parameters parameters, BiConsumer<HashKey, List<Msg>> process) {
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, service,
                                                                       r -> new LinearServer(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       LinearClient.getCreate(null));
        this.params = parameters;
        this.process = process;
    }

    public void clear() {
        pause();
        comm.set(null);
        order.set(null);
        messenger.set(null);
        nextView.set(null);
        nextViewConsensusKeyPair.set(null);
    }

    public CommonCommunications<LinearClient, Service> getComm() {
        return comm.get();
    }

    public ViewContext getContext() {
        return context.get();
    }

    public ViewMember getNextView() {
        return nextView.get();
    }

    public KeyPair getNextViewConsensusKeyPair() {
        return nextViewConsensusKeyPair.get();
    }

    public void joinMessageGroup(ViewContext newView, TickScheduler scheduler, BiConsumer<HashKey, List<Msg>> process) {
        log.debug("Joining message group: {} on: {}", newView.getId(), newView.getMember());
        Messenger nextMsgr = newView.createMessenger(params, params.dispatcher);
        messenger.set(nextMsgr);
        nextMsgr.register(round -> scheduler.tick());
        order.set(new MemberOrder(process, nextMsgr));
    }

    public KeyPair nextViewConsensusKey() {
        KeyPair current = nextViewConsensusKeyPair.get();

        KeyPair keyPair = generateKeyPair(2048, "RSA");
        nextViewConsensusKeyPair.set(keyPair);
        byte[] encoded = keyPair.getPublic().getEncoded();
        byte[] signed = sign(params.signature.get(), encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", params.member);
            return null;
        }
        nextView.set(ViewMember.newBuilder()
                               .setId(params.member.getId().toByteString())
                               .setConsensusKey(ByteString.copyFrom(encoded))
                               .setSignature(ByteString.copyFrom(signed))
                               .build());
        if (log.isTraceEnabled()) {
            log.trace("Generating next view consensus key current: {} next: {} on: {}",
                      current == null ? null : new HashKey(Conversion.hashOf(current.getPublic().getEncoded())),
                      new HashKey(Conversion.hashOf(keyPair.getPublic().getEncoded())), params.member);
        }
        return current;
    }

    public void pause() {
        CommonCommunications<LinearClient, Service> currentComm = comm.get();
        if (currentComm != null) {
            ViewContext current = context.get();
            assert current != null : "No current view, but comm exists!";
            currentComm.deregister(current.getId());
        }
        MemberOrder currentTotalOrder = order.get();
        if (currentTotalOrder != null) {
            currentTotalOrder.stop();
        }
        Messenger currentMessenger = messenger.get();
        if (currentMessenger != null) {
            currentMessenger.stop();
        }
    }

    public void publish(Message message) {
        final Messenger currentMsgr = messenger.get();
        if (currentMsgr == null) {
            log.trace("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message);
    }

    public void resume(Service service) {
        resume(service, params.gossipDuration, params.scheduler);
    }

    public void setContext(ViewContext vc) {
        context.set(vc);
    }

    /**
     * Ye Jesus Nut
     */
    public void viewChange(ViewContext newView, TickScheduler scheduler, int currentRegent, Service service,
                           boolean resume) {
        pause();

        log.info("Installing new view: {} rings: {} ttl: {} on: {} regent: {} member: {} view member: {}",
                 newView.getId(), newView.getRingCount(), newView.timeToLive(), newView.getMember(),
                 currentRegent >= 0 ? newView.getRegent(currentRegent) : "None", newView.isMember(),
                 newView.isViewMember());

        comm.set(createClientComms.apply(newView.getId()));
        messenger.set(null);
        order.set(null);
        setContext(newView);
        if (newView.isViewMember()) {
            joinMessageGroup(newView, scheduler, process);
        }

        if (resume) {
            resume(service);
        }
    }

    private void resume(Service service, Duration gossipDuration, ScheduledExecutorService scheduler) {
        CommonCommunications<LinearClient, Service> currentComm = getComm();
        if (currentComm != null) {
            ViewContext current = getContext();
            assert current != null : "No current view, but comm exists!";
            currentComm.register(current.getId(), service);
        }
        MemberOrder currentTO = order.get();
        if (currentTO != null) {
            currentTO.start();
        }
        Messenger currentMsg = messenger.get();
        if (currentMsg != null) {
            currentMsg.start(gossipDuration, scheduler);
        }
    }
}
