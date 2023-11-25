package com.salesforce.apollo.bloomFilters;

import com.salesfoce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hal.hildebrand
 **/
public class BloomWindowTest {

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var algo = DigestAlgorithm.DEFAULT;

        var window = 1 << 16;
        var seen = BloomWindow.create(entropy.nextLong(), entropy.nextLong(), window, Math.pow(10, -9),
                                      Biff.Type.DIGEST);
        var inserted = new TreeSet<Digest>();
        var falsePositives = new TreeSet<Digest>();
        var decayed = new TreeSet<Digest>();

        IntStream.range(0, window).mapToObj(i -> algo.random(entropy)).forEach(d -> {
            if (!seen.add(d)) {
                falsePositives.add(d);
            } else {
                inserted.add(d);
            }
        });
        assertEquals(0, falsePositives.size(), falsePositives.toString());

        for (Digest digest : inserted) {
            if (!seen.contains(digest)) {
                decayed.add(digest);
            }
        }
        assertEquals(0, decayed.size(), decayed.toString());

        IntStream.range(0, window).mapToObj(i -> algo.random(entropy)).forEach(d -> {
            if (seen.contains(d)) {
                falsePositives.add(d);
            }
        });
        assertEquals(0, falsePositives.size(), falsePositives.toString());

        var overflow = new ArrayList<Digest>();

        IntStream.range(0, window).mapToObj(i -> algo.random(entropy)).forEach(d -> {
            if (!seen.add(d)) {
                falsePositives.add(d);
            } else {
                overflow.add(d);
            }
        });
        assertEquals(1, falsePositives.size(), falsePositives.toString());

        for (Digest digest : overflow) {
            if (!seen.contains(digest)) {
                decayed.add(digest);
            }
        }
        assertEquals(0, decayed.size(), decayed.toString());

        for (Digest digest : inserted) {
            if (!seen.contains(digest)) {
                decayed.add(digest);
            }
        }
        assertEquals(0, decayed.size(), decayed.toString());

        IntStream.range(0, window).mapToObj(i -> algo.random(entropy)).forEach(d -> {
            if (seen.contains(d)) {
                falsePositives.add(d);
            }
        });
        assertEquals(12, falsePositives.size(), falsePositives.toString());
    }
}
