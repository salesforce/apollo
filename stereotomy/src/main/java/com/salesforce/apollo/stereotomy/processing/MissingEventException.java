package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

public class MissingEventException extends KeyEventProcessingException {

    private static final long      serialVersionUID = 1L;
    private final EventCoordinates missingEvent;

    public MissingEventException(KeyEvent dependingEvent, EventCoordinates missingEvent) {
        super(dependingEvent);
        this.missingEvent = missingEvent;
    }

    public EventCoordinates missingEvent() {
        return this.missingEvent;
    }

}
