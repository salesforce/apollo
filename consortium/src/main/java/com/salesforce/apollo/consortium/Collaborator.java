/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Collaborator {

    void awaitSynchronization();

    void cancel(Timers t);

    void changeRegency(List<EnqueuedTransaction> transactions);

    void delay(Message message, Member from);

    void deliverBlock(Block block, Member from);

    void deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from);

    void deliverStop(Stop data, Member from);

    void deliverStopData(StopData stopData, Member from);

    void deliverSync(Sync syncData, Member from);

    void deliverValidate(Validate v);

    void establishGenesisView();

    void establishNextRegent();

    void generateBlock();

    void generateView();

    int getCurrentRegent();

    Member getMember();

    void initializeConsensus();

    boolean isRegent(int regency);

    void join();

    void joinView();

    int nextRegent();

    void receive(ReplicateTransactions transactions, Member from);

    void receive(Transaction txn);

    boolean receive(Transaction txn, boolean replicated);

    void receiveJoin(Transaction txn);

    void receiveJoins(ReplicateTransactions transactions, Member from);

    void recover(HashedCertifiedBlock anchor);

    void reschedule(List<EnqueuedTransaction> transactions);

    void resolveRegentStatus();

    void scheduleCheckpointBlock();

    void shutdown(); 

    void synchronize(int elected, Map<Member, StopData> regencyData);

    void totalOrderDeliver();

    void cancelSynchronization();

}
