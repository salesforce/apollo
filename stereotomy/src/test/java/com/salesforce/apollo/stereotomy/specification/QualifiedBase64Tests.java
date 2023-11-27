package com.salesforce.apollo.stereotomy.specification;

import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64Length;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.basicIdentifierPlaceholder;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.selfSigningIdentifierPlaceholder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier;

public class QualifiedBase64Tests {

    @Test
    public void test__basicIdentifierPlaceholder() {
        for (var a : SignatureAlgorithm.values()) {
            var placeholder = basicIdentifierPlaceholder(a);
            assertEquals(qb64Length(a.publicKeyLength()), placeholder.length(), a.name() + " placeholder length");
            assertTrue(placeholder.matches("^#+$"), "contains only #");
        }
    }

    @Test
    public void test__selfAddressingIdentifierPlaceholder() {
        for (DigestAlgorithm a : DigestAlgorithm.values()) {
            String placeholder = QualifiedBase64Identifier.selfAddressingIdentifierPlaceholder(a);
            assertEquals(qb64Length(a.digestLength()), placeholder.length(), a.name() + " placeholder length");
            assertTrue(placeholder.matches("^#+$"), "contains only #");
        }
    }

    @Test
    public void test__selfSigningIdentifierPlaceholder() {
        for (var a : SignatureAlgorithm.values()) {
            var placeholder = selfSigningIdentifierPlaceholder(a);
            assertEquals(qb64Length(a.signatureLength()), placeholder.length(), a.name() + " placeholder length");
            assertTrue(placeholder.matches("^#+$"), "contains only #");
        }
    }

}
