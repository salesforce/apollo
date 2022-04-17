package com.salesforce.apollo.ethereal.rbc;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Have;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.crypto.Digest;

public interface Processor {

    /**
     * Final phase; update the commit, prevote and unit state from the supplied
     * update
     * 
     * @param update - the Update from our partner
     */
    void updateFrom(Update update);

    /**
     * First phase request. Answer the gossip for the current state of the receiver
     * 
     * @param context - the digest id of the context for routing
     * @param ring    - the ring we're gossiping on
     * @return the Gossip
     */
    Gossip gossip(Digest context, int ring);

    /**
     * First phase reply. Answer the Update from the receiver's state, based on the
     * suppled Have
     * 
     * @param have - the state contained by the partner
     * @return the Update based on the current state of the receiver and the have
     *         state of the partner
     */
    Update gossip(Have have);

    /**
     * Second phase, update the receiver state from the supplied update. Return an
     * update based on the current state and the haves of the supplied update
     * 
     * @param update - the Update from the partner
     * @return the Update from the current state of the receiver and the have state
     *         of the supplied update
     */
    Update update(Update update);
}
