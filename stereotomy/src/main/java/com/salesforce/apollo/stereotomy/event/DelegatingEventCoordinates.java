package com.salesforce.apollo.stereotomy.event;

import org.joou.ULong;

import com.salesfoce.apollo.stereotomy.event.proto.DelegatingEventCoords;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

public class DelegatingEventCoordinates {

    private final Identifier       identifier;
    private final String           ilk;
    private final EventCoordinates previousEvent;
    private final ULong            sequenceNumber;

    public DelegatingEventCoordinates(DelegatingEventCoords coordinates) {
        identifier = Identifier.from(coordinates.getIdentifier());
        sequenceNumber = ULong.valueOf(coordinates.getSequenceNumber());
        previousEvent = new EventCoordinates(coordinates.getPrevious());
        ilk = coordinates.getIlk();
    }

    public DelegatingEventCoordinates(String ilk, Identifier identifier, ULong sequenceNumber,
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

    public ULong getSequenceNumber() {
        return sequenceNumber;
    }

    public DelegatingEventCoords toCoords() {
        return DelegatingEventCoords.newBuilder()
                                    .setIdentifier(identifier.toIdent())
                                    .setSequenceNumber(sequenceNumber.longValue())
                                    .setPrevious(previousEvent.toEventCoords())
                                    .setIlk(ilk)
                                    .build();
    }

}
