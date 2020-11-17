/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.KeyPair;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.MemberOrder;
import com.salesforce.apollo.membership.messaging.Messenger;

/**
 * Volatile state consolidation for Conosortium
 *
 * @author hal.hildebrand
 *
 */
class VolatileState implements MembershipListener<Member> {
    private volatile CommonCommunications<ConsortiumClientCommunications, Service> comm;
    private volatile KeyPair                                                       consensusKeyPair;
    private volatile CurrentBlock                                                  current;
    private volatile Messenger                                                     messenger;
    private volatile ViewMember                                                    nextView;
    private volatile KeyPair                                                       nextViewConsensusKeyPair;
    private volatile MemberOrder                                                   order;
    private volatile Validator                                                     validator;

    @Override
    public void fail(Member member) {
        final Context<Member> view = getCurrentView();
        if (view != null) {
            view.offlineIfActive(member.getId());
        }
    }

    @Override
    public void recover(Member member) {
        final Context<Member> view = getCurrentView();
        if (view != null) {
            view.activateIfOffline(member.getId());
        }
    }

    void clear() {
        pause();
        comm = null;
        order = null;
        consensusKeyPair = null;
        current = null;
        messenger = null;
        nextView = null;
        order = null;
        validator = validator;
    }

    CommonCommunications<ConsortiumClientCommunications, Service> getComm() {
        final CommonCommunications<ConsortiumClientCommunications, Service> cc = comm;
        return cc;
    }

    KeyPair getConsensusKeyPair() {
        final KeyPair ckp = consensusKeyPair;
        return ckp;
    }

    CurrentBlock getCurrent() {
        final CurrentBlock cb = current;
        return cb;
    }

    Context<Member> getCurrentView() {
        Validator v = getValidator();
        return v != null ? v.getView() : null;
    }

    Member getLeader() {
        return getValidator().getLeader();
    }

    Messenger getMessenger() {
        Messenger currentMsgr = messenger;
        return currentMsgr;
    }

    ViewMember getNextView() {
        final ViewMember c = nextView;
        return c;
    }

    KeyPair getNextViewConsensusKeyPair() {
        final KeyPair c = nextViewConsensusKeyPair;
        return c;
    }

    MemberOrder getOrder() {
        final MemberOrder cTo = order;
        return cTo;
    }

    Validator getValidator() {
        final Validator v = validator;
        return v;
    }

    void pause() {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = getComm();
        if (currentComm != null) {
            Context<Member> current = getCurrentView();
            assert current != null : "No current view, but comm exists!";
            currentComm.deregister(current.getId());
        }

        MemberOrder currentTotalOrder = getOrder();
        if (currentTotalOrder != null) {
            currentTotalOrder.stop();
        }
        Messenger currentMessenger = getMessenger();
        if (currentMessenger != null) {
            currentMessenger.stop();
        }
    }

    void resume(Service service, Duration gossipDuration, ScheduledExecutorService scheduler) {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = getComm();
        if (currentComm != null) {
            Context<Member> current = getCurrentView();
            assert current != null : "No current view, but comm exists!";
            currentComm.register(current.getId(), service);
        }
        MemberOrder currentTO = getOrder();
        if (currentTO != null) {
            currentTO.start();
        }
        Messenger currentMsg = getMessenger();
        if (currentMsg != null) {
            currentMsg.start(gossipDuration, scheduler);
        }
    }

    void setComm(CommonCommunications<ConsortiumClientCommunications, Service> comm) {
        this.comm = comm;
    }

    void setConsensusKeyPair(KeyPair consensusKeyPair) {
        this.consensusKeyPair = consensusKeyPair;
    }

    void setCurrent(CurrentBlock current) {
        this.current = current;
    }

    void setMessenger(Messenger messenger) {
        this.messenger = messenger;
    }

    void setNextView(ViewMember nextView) {
        this.nextView = nextView;
    }

    void setNextViewConsensusKeyPair(KeyPair nextViewConsensusKeyPair) {
        this.nextViewConsensusKeyPair = nextViewConsensusKeyPair;
    }

    void setOrder(MemberOrder order) {
        this.order = order;
    }

    void setValidator(Validator validator) {
        this.validator = validator;
    }

}
