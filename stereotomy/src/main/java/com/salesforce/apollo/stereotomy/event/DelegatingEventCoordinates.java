package com.salesforce.apollo.stereotomy.event;

import com.salesfoce.apollo.stereotomy.event.proto.DelegatingEventCoords;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

public class DelegatingEventCoordinates {

    private final Identifier       identifier;
    private final String           ilk;
    private final EventCoordinates previousEvent;
    private final long             sequenceNumber;

    public DelegatingEventCoordinates(DelegatingEventCoords coordinates) {
        identifier = Identifier.from(coordinates.getIdentifier());
        sequenceNumber = coordinates.getSequenceNumber();
        previousEvent = new EventCoordinates(coordinates.getPrevious());
        ilk = coordinates.getIlk();
    }

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

    public DelegatingEventCoords toCoords() {
        return DelegatingEventCoords.newBuilder()
                                    .setIdentifier(identifier.toIdent())
                                    .setSequenceNumber(sequenceNumber)
                                    .setPrevious(previousEvent.toEventCoords())
                                    .setIlk(ilk)
                                    .build();
    }

}
