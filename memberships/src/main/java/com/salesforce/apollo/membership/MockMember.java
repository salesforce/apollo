package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;

import java.io.InputStream;

/**
 * A stand in for comparison functions
 *
 * @author hal.hildebrand
 **/
public class MockMember implements Member {
    private final Digest id;

    public MockMember(Digest id) {
        this.id = id;
    }

    @Override
    public int compareTo(Member o) {
        return id.compareTo(o.getId());
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return null;
    }

    @Override
    public Digest getId() {
        return id;
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return false;
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return false;
    }
}
