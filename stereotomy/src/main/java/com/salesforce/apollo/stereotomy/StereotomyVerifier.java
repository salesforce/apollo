package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

import java.io.InputStream;
import java.util.Optional;

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
        var verifier = verifierFor(signature.getSequenceNumber());
        return verifier.isEmpty() ? new Filtered(false, 0,
                                                 new JohnHancock(signature.getAlgorithm(), new byte[] {}, ULong.MIN))
                                  : verifier.get().filtered(threshold, signature, message);
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        var verifier = verifierFor(signature.getSequenceNumber());
        return verifier.isEmpty() ? false : verifier.get().verify(signature, message);
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        var verifier = verifierFor(signature.getSequenceNumber());
        return verifier.isEmpty() ? false : verifier.get().verify(threshold, signature, message);
    }

    private Optional<Verifier> verifierFor(ULong sequenceNumber) {
        KeyState keyState = stereotomy.kerl.getKeyState(identifier, sequenceNumber);
        if (keyState == null) {
            return Optional.empty();
        }
        return Optional.of(new DefaultVerifier(keyState.getKeys()));
    }

    public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
        KeyState keyState = stereotomy.kerl.getKeyState(coordinates);
        if (keyState == null) {
            return Optional.empty();
        }
        return Optional.of(new DefaultVerifier(keyState.getKeys()));
    }
}
