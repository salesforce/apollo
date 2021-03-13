package io.quantumdb.core.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperations;

public class ChangelogTest {

    @Test
    public void testAddingChangeSet() {
        Changelog changelog = new Changelog();
        RenameTable renameTable = SchemaOperations.renameTable("users", "customers");
        changelog.addChangeSet("test", "Michael de Jong", renameTable);

        Version version = changelog.getLastAdded();
        Version lookup = changelog.getVersion(version.getId());

        assertEquals(version, lookup);
    }

    @Test
    public void testRetrievingVersion() {
        Changelog changelog = new Changelog();
        Version version = changelog.getLastAdded();
        Version lookup = changelog.getVersion(version.getId());

        assertEquals(version, lookup);
    }

    @Test
    public void testSingleArgumentConstructorWithVersionId() {
        Changelog changelog = new Changelog("version-1");

        assertNotNull(changelog.getRoot());
        assertEquals(changelog.getRoot(), changelog.getLastAdded());
        assertEquals("version-1", changelog.getRoot().getId());
        assertNull(changelog.getRoot().getChild());
        assertNull(changelog.getRoot().getParent());
    }

    @Test
    public void testThatSingleArgumentConstructorWithEmptyStringThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new Changelog(""));
    }

    @Test
    public void testThatSingleArgumentConstructorWithNullInputThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new Changelog(null));
    }

    @Test
    public void testZeroArgumentConstructor() {
        Changelog changelog = new Changelog();

        assertNotNull(changelog.getRoot());
        assertEquals(changelog.getRoot(), changelog.getLastAdded());
    }

}
