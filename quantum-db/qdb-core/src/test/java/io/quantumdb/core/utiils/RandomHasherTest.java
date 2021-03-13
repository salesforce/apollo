package io.quantumdb.core.utiils;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;

public class RandomHasherTest {

    @Test
    public void testGeneratingRandomHashReturnsDifferentStringsUponMultipleCalls() {
        String hash1 = RandomHasher.generateHash();
        String hash2 = RandomHasher.generateHash();
        assertNotEquals(hash1, hash2);
    }

    @Test
    public void testGeneratingRandomHashReturnsNonEmptyString() {
        String hash = RandomHasher.generateHash();
        assertFalse(isNullOrEmpty(hash));
    }

    @Test
    public void testGeneratingUniqueRefIdWithEmptyTableMapping() {
        RefLog refLog = new RefLog();
        String refId = RandomHasher.generateRefId(refLog);
        assertFalse(isNullOrEmpty(refId));
    }

    @Test
    public void testGeneratingUniqueRefIdWithFilledTableMapping() {
        RefLog refLog = new RefLog();
        Version version = new Version(RandomHasher.generateHash(), null);
        refLog.addTable("users", "users", version, Lists.newArrayList());

        String refId = RandomHasher.generateRefId(refLog);
        assertFalse(isNullOrEmpty(refId));
        assertNotEquals("users", refId);
    }

}
