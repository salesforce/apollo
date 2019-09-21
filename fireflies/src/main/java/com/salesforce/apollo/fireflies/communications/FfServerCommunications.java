/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.communications;

import java.security.cert.X509Certificate;
import java.util.UUID;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.Digests;
import com.salesforce.apollo.avro.Gossip;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Update;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.Fireflies;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfServerCommunications implements Fireflies {

    private final X509Certificate certificate;
    private final UUID remoteMemberId;
    private final Service service;

    public FfServerCommunications(Service view, X509Certificate certificate) {
        assert view != null : "View cannot be null";
        assert certificate != null : "Certificate cannot be null";
        this.service = view;
        this.remoteMemberId = Member.getMemberId(certificate);
        this.certificate = certificate;
    }

    public UUID getRemoteMemberId() {
        return remoteMemberId;
    }

    @Override
    public Gossip gossip(Signed note, int ring, Digests digests) throws AvroRemoteException {
        return service.rumors(ring, digests, remoteMemberId, certificate, note);
    }

    @Override
    public int ping(int ping) throws AvroRemoteException {
        return 200; // we handle the ping here - no need for the view to get involved
    }

    @Override
    public void update(int ring, Update update) {
        service.update(ring, update, remoteMemberId);
    }
}
