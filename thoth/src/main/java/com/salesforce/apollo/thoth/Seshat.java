package com.salesforce.apollo.thoth;

import java.io.InputStream;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.stereotomy.EventCoordinates;

public interface Seshat {
    Seshat NONE = new Seshat() {

        @Override
        public Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                 InputStream message) {
            return null;
        }

        @Override
        public boolean validate(EventCoordinates coordinates) {
            return true;
        }

        @Override
        public boolean verify(EventCoordinates coordinates, JohnHancock from, ByteString byteString) {
            return true;
        }

        @Override
        public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
            return true;
        }

        @Override
        public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                              InputStream message) {
            return true;
        }
    };

    Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                      InputStream message);

    boolean validate(EventCoordinates coordinates);

    boolean verify(EventCoordinates coordinates, JohnHancock from, ByteString byteString);

    boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message);

    boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                   InputStream message);

}
