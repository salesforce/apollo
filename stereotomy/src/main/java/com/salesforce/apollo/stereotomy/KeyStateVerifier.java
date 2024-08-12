package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

import java.io.InputStream;
import java.util.Optional;

/**
 * An abstract verifier that uses KeyState
 *
 * @author hal.hildebrand
 **/
public abstract class KeyStateVerifier<D extends Identifier> implements Verifier {
    protected final D identifier;

    public KeyStateVerifier(D identifier) {
        this.identifier = identifier;
    }

    public D identifier() {
        return identifier;
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        var verifier = verifierFor(signature.getSequenceNumber());
        return verifier.isPresent() && verifier.get().verify(threshold, signature, message);
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        var verifier = verifierFor(signature.getSequenceNumber());
        return verifier.isPresent() && verifier.get().verify(signature, message);
    }

    protected abstract KeyState getKeyState(ULong sequenceNumber);

    protected Optional<Verifier> verifierFor(ULong sequenceNumber) {
        KeyState keyState = getKeyState(sequenceNumber);
        if (keyState == null) {
            return Optional.empty();
        }
        return Optional.of(new DefaultVerifier(keyState.getKeys()));
    }
}
