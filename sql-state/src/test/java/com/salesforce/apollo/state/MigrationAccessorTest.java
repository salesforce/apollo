/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.bouncycastle.util.Strings;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

/**
 * @author hal.hildebrand
 *
 */
public class MigrationAccessorTest {

    @Test
    public void smoke() throws Exception {
        final var baos = new ByteArrayOutputStream();
        JarOutputStream jos = new JarOutputStream(baos);
        jos.putNextEntry(new ZipEntry("a/"));
        jos.putNextEntry(new ZipEntry("a/0.txt"));
        jos.write(Strings.toByteArray("hello 0"));
        jos.closeEntry();
        jos.putNextEntry(new ZipEntry("a/b/"));
        jos.putNextEntry(new ZipEntry("a/b/1.txt"));
        jos.write(Strings.toByteArray("hello 1"));
        jos.closeEntry();
        jos.putNextEntry(new ZipEntry("a/b/2.txt"));
        jos.write(Strings.toByteArray("hello 2"));
        jos.closeEntry();
        jos.close();
        final var ra = new MigrationAccessor(ByteString.copyFrom(baos.toByteArray()));

        assertEquals("MigrationAccessor{}", ra.describe());
        final var locs = ra.describeLocations();
        assertNotNull(locs);
        assertEquals(1, locs.size());
        assertTrue(locs.contains("/"));
        final var os = ra.openStream(null, "a/0.txt");
        assertNotNull(os);
        final var testy = new ByteArrayOutputStream();
        os.transferTo(testy);
        assertEquals("hello 0", testy.toString());

        final var listing = ra.list(null, "/", true, true, false);
        assertNotNull(listing);
        assertEquals(3, listing.size());
        Arrays.asList("a/0.txt", "a/b/1.txt", "a/b/2.txt").forEach(s -> assertTrue(listing.contains(s)));
    }
}
