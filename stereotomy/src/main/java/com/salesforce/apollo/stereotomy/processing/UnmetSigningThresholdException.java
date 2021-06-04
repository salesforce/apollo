package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.event.KeyEvent;

public class UnmetSigningThresholdException extends KeyEventProcessingException {

    private static final long serialVersionUID = 1L;

    public UnmetSigningThresholdException(KeyEvent keyEvent) {
        super(keyEvent);
    }

}
