package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.event.AttachmentEvent;

public class AttachmentEventProcessingException extends RuntimeException {

    private static final long     serialVersionUID = 1L;
    private final AttachmentEvent attachmentEvent;

    public AttachmentEventProcessingException(AttachmentEvent attachmentEvent) {
        this.attachmentEvent = attachmentEvent;
    }

    public AttachmentEvent attachmentEvent() {
        return this.attachmentEvent;
    }

}
