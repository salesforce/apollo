package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

/**
 * A Verifier that uses the backing Stereotomy for keys used for Signature verification for the Identifier
 *
 * @author hal.hildebrand
 **/
public class KerlVerifier<D extends Identifier> extends KeyStateVerifier<D> {

    private final KERL kerl;

    public KerlVerifier(D identifier, KERL kerl) {
        super(identifier);
        this.kerl = kerl;
    }

    @Override
    protected KeyState getKeyState(ULong sequenceNumber) {
        return kerl.getKeyState(identifier, sequenceNumber);
    }

    @Override
    protected KeyState getKeyState(EventCoordinates coordinates) {
        return kerl.getKeyState(coordinates);
    }
}
