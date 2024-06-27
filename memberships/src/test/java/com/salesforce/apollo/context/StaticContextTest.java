package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author hal.hildebrand
 **/
public class StaticContextTest {
    @Test
    public void consistency() throws Exception {
        var prototype = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 10, 0.2, 2);
        List<SigningMember> members = new ArrayList<>();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        for (int i = 0; i < 10; i++) {
            SigningMember m = new ControlledIdentifierMember(stereotomy.newIdentifier());
            members.add(m);
            prototype.activate(m);
        }

        var context = prototype.asStatic();

        var predecessors = context.predecessors(members.get(0).getId());
        assertEquals(members.get(3), predecessors.get(2));

        var successors = context.successors(members.get(1).getId());
        assertEquals(members.get(8), successors.get(0));
        assertEquals(members.get(9), context.successor(1, members.get(0).getId()));
    }

    @Test
    public void lookupMember() throws Exception {
        var prototype = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 10, 0.2, 2);
        List<SigningMember> members = new ArrayList<>();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        for (int i = 0; i < 10; i++) {
            SigningMember m = new ControlledIdentifierMember(stereotomy.newIdentifier());
            members.add(m);
            prototype.activate(m);
        }

        var context = prototype.asStatic();

        var m = context.getMember(members.get(0).getId());
        assertNotNull(m);
        assertEquals(members.get(0), m);
    }
}
