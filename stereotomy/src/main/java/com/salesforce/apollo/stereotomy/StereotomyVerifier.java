package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.Validator;
import org.joou.ULong;

import java.io.InputStream;

/**
 * A Verifier that uses the backing Stereotomy for keys used for Signature verification for the Identifier
 *
 * @author hal.hildebrand
 **/
public class StereotomyVerifier<D extends Identifier> implements Verifier {

    private final D              identifier;
    private final StereotomyImpl stereotomy;

    public StereotomyVerifier(D identifier, StereotomyImpl stereotomy) {
        this.identifier = identifier;
        this.stereotomy = stereotomy;
    }

    public D identifier() {
        return identifier;
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return null;
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return false;
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return false;
    }

    private Validator validatorFor(ULong sequenceNumber) {
        KeyState keyState = stereotomy.kerl.getKeyState(identifier, sequenceNumber);
        return new KeyStateValidator(keyState);
    }
}
