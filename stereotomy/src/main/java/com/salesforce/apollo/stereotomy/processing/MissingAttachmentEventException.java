package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;

public class MissingAttachmentEventException extends RuntimeException {

    private static final long      serialVersionUID = 1L;
    private final EventCoordinates missingEvent;
    private final AttachmentEvent  attachment;

    public MissingAttachmentEventException(AttachmentEvent attachment, EventCoordinates missingEvent) {
        this.attachment = attachment;
        this.missingEvent = missingEvent;
    }

    public EventCoordinates missingEvent() {
        return this.missingEvent;
    }

    public AttachmentEvent attachement() {
        return attachment;
    }

}
