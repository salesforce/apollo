package com.salesforce.apollo.stereotomy.processing;

import static java.util.Objects.requireNonNull;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;

public class MissingReferencedEventException extends AttachmentEventProcessingException {

    private static final long      serialVersionUID = 1L;
    private final EventCoordinates referencedEvent;

    public MissingReferencedEventException(AttachmentEvent attachmentEvent, EventCoordinates referencedEvent) {
        super(attachmentEvent);
        this.referencedEvent = requireNonNull(referencedEvent);
    }

    public EventCoordinates referencedEvent() {
        return this.referencedEvent;
    }

}
