package com.salesforce.apollo.stereotomy.event;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

public class DelegatingEventCoordinates {
    public static DelegatingEventCoordinates from(ByteBuffer buff) {
        byte[] ilk = new byte[3];
        buff.get(ilk);
        Identifier identifier = Identifier.from(buff);
        long sn = buff.getLong();
        EventCoordinates coordinates = EventCoordinates.from(buff);
        return new DelegatingEventCoordinates(new String(ilk), identifier, sn, coordinates);
    }

    public static EventCoordinates from(ByteString bs) {
        return EventCoordinates.from(bs.asReadOnlyByteBuffer());
    }

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

    public ByteString toByteString() {
        ByteBuffer sn = ByteBuffer.wrap(new byte[8]);
        sn.putLong(sequenceNumber);
        sn.flip();
        return ByteString.copyFromUtf8(ilk)
                         .concat(identifier.toByteString())
                         .concat(previousEvent.toByteString())
                         .concat(ByteString.copyFrom(sn));
    }

}
