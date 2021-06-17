package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.stereotomy.identifier.Identifier;

public class DelegatingEventCoordinates {

    private final Identifier       identifier;
    private final String           ilk;
    private final EventCoordinates previousEvent;
    private final long             sequenceNumber;

    public DelegatingEventCoordinates(String ilk, Identifier identifier, long sequenceNumber,
            EventCoordinates previousEvent) {
        this.identifier = identifier;
        this.sequenceNumber = sequenceNumber;
        this.previousEvent = previousEvent;
        this.ilk = ilk;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public String getIlk() {
        return ilk;
    }

    public EventCoordinates getPreviousEvent() {
        return previousEvent;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

}
