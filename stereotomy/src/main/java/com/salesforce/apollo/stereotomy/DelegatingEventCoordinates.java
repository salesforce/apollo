package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

public class DelegatingEventCoordinates {

    private final Identifier       identifier;
    private final EventCoordinates previousEvent;
    private final long             sequenceNumber;

    public DelegatingEventCoordinates(Identifier identifier, long sequenceNumber, EventCoordinates previousEvent) {
        this.identifier = identifier;
        this.sequenceNumber = sequenceNumber;
        this.previousEvent = previousEvent;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public EventCoordinates getPreviousEvent() {
        return previousEvent;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

}
