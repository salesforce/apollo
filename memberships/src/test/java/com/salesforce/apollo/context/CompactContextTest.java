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

/**
 * @author hal.hildebrand
 **/
public class CompactContextTest {
    @Test
    public void consistency() throws Exception {
        var context = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 10, 0.2, 2);
        List<SigningMember> members = new ArrayList<>();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        for (int i = 0; i < 10; i++) {
            SigningMember m = new ControlledIdentifierMember(stereotomy.newIdentifier());
            members.add(m);
            context.activate(m);
        }

        var compact = new CompactContext(context);

        var predecessors = compact.predecessors(members.get(0).getId());
        assertEquals(members.get(9).getId(), predecessors.get(2));

        var successors = compact.successors(members.get(1).getId());
        assertEquals(members.get(0).getId(), successors.get(0));
        assertEquals(members.get(1).getId(), compact.ring(1).successor(members.get(0).getId()));
    }
}