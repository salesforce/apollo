package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.KeyEvent;

public class UnmetWitnessThresholdException extends KeyEventProcessingException {

    private static final long serialVersionUID = 1L;

    public UnmetWitnessThresholdException(KeyEvent keyEvent) {
        super(keyEvent);
    }

}
