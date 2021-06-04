package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.KeyEvent;

public class MissingDelegatingEventException extends KeyEventProcessingException {

    private static final long                serialVersionUID = 1L;
    private final DelegatingEventCoordinates missingEvent;

    public MissingDelegatingEventException(KeyEvent dependingEvent, DelegatingEventCoordinates missingEvent) {
        super(dependingEvent);
        this.missingEvent = missingEvent;
    }

    public DelegatingEventCoordinates missingEvent() {
        return this.missingEvent;
    }

}
