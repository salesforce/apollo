/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import static com.salesforce.apollo.crypto.QualifiedBase64.base64;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64Length;
import static com.salesforce.apollo.crypto.QualifiedBase64.unbase64;
import static com.salesforce.apollo.crypto.QualifiedBase64.unbase64Int;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;

import org.junit.jupiter.api.Test;

public class QualifiedBase64Tests {

    @Test
    public void test__base64__bytea() {
        assertEquals("AA", base64(new byte[] { 0x00 }));
        assertEquals("AAE", base64(new byte[] { 0x00, 0x01 }));
        assertEquals("AAEC", base64(new byte[] { 0x00, 0x01, 0x02 }));
        assertEquals("AAECAw", base64(new byte[] { 0x00, 0x01, 0x02, 0x03 }));
    }

    @Test
    public void test__unbase64() {
        assertArrayEquals(new byte[] { 0x00 }, unbase64("AA"));
        assertArrayEquals(new byte[] { 0x00, 0x01 }, unbase64("AAE"));
        assertArrayEquals(new byte[] { 0x00, 0x01, 0x02 }, unbase64("AAEC"));
        assertArrayEquals(new byte[] { 0x00, 0x01, 0x02, 0x03 }, unbase64("AAECAw"));
    }

    @Test
    public void test__base64__int() {
        assertEquals("A", base64(0));
        assertEquals("B", base64(1));
        assertEquals("C", base64(2));
        assertEquals("b", base64(27));
        assertEquals("-", base64(62));
        assertEquals("_", base64(63));
        assertEquals("BA", base64(64));
        assertEquals("BQ", base64(80));
        assertEquals("B-", base64(126));
        assertEquals("B_", base64(127));
        assertEquals("CA", base64(128));
        assertEquals("__", base64(4095));
        assertEquals("BAA", base64(4096));
        assertEquals("Bd7", base64(6011));
    }

    @Test
    public void test__base64__int_padding() {
        assertEquals("AA", base64(0, 2));
        assertEquals("AAA", base64(0, 3));
        assertEquals("AAAA", base64(0, 4));
        assertEquals("AB", base64(1, 2));
        assertEquals("AC", base64(2, 2));
        assertEquals("Ab", base64(27, 2));
        assertEquals("A-", base64(62, 2));
        assertEquals("A_", base64(63, 2));
        assertEquals("BA", base64(64, 2));
        assertEquals("BQ", base64(80, 2));
        assertEquals("B-", base64(126, 2));
        assertEquals("B_", base64(127, 2));
        assertEquals("CA", base64(128, 2));
        assertEquals("__", base64(4095, 2));
        assertEquals("BAA", base64(4096, 2));
        assertEquals("Bd7", base64(6011, 2));
    }

    @Test
    public void test__unbase64Int() {
        assertEquals(0, unbase64Int("A"));
        assertEquals(1, unbase64Int("B"));
        assertEquals(2, unbase64Int("C"));
        assertEquals(27, unbase64Int("b"));
        assertEquals(62, unbase64Int("-"));
        assertEquals(63, unbase64Int("_"));
        assertEquals(64, unbase64Int("BA"));
        assertEquals(80, unbase64Int("BQ"));
        assertEquals(126, unbase64Int("B-"));
        assertEquals(127, unbase64Int("B_"));
        assertEquals(128, unbase64Int("CA"));
        assertEquals(4095, unbase64Int("__"));
        assertEquals(4096, unbase64Int("BAA"));
        assertEquals(6011, unbase64Int("Bd7"));
    }

    @Test
    public void test__qb64Length() {
        var enc = Base64.getUrlEncoder();
        assertEquals(enc.encodeToString(new byte[0]).length() + 4, qb64Length(0));
        assertEquals(enc.encodeToString(new byte[1]).length(), qb64Length(1));
        assertEquals(enc.encodeToString(new byte[2]).length(), qb64Length(2));
        assertEquals(enc.encodeToString(new byte[3]).length() + 4, qb64Length(3));
        assertEquals(enc.encodeToString(new byte[4]).length(), qb64Length(4));
        assertEquals(enc.encodeToString(new byte[5]).length(), qb64Length(5));
        assertEquals(enc.encodeToString(new byte[6]).length() + 4, qb64Length(6));
        assertEquals(enc.encodeToString(new byte[7]).length(), qb64Length(7));
        assertEquals(enc.encodeToString(new byte[8]).length(), qb64Length(8));
        assertEquals(enc.encodeToString(new byte[9]).length() + 4, qb64Length(9));
    }
}
