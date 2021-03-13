package io.quantumdb.core.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ChangeSetTest {

    @Test
    public void testComparison() throws InterruptedException {
        ChangeSet oldest = new ChangeSet("test", "Michael de Jong");

        Thread.sleep(100);
        ChangeSet newest = new ChangeSet("test", "Michael de Jong");

        assertEquals(-1, oldest.compareTo(newest));
    }

    @Test
    public void testSingleArgumentConstructor() {
        ChangeSet changeSet = new ChangeSet("test", "Michael de Jong");

        assertEquals("Michael de Jong", changeSet.getAuthor());
        assertNull(changeSet.getDescription());
        assertNotNull(changeSet.getCreated());
    }

    @Test
    public void testThatDoubleArgumentConstructorWithEmptyStringArgumentIsAllowed() {
        ChangeSet changeSet = new ChangeSet("test", "Michael de Jong", "");

        assertEquals("Michael de Jong", changeSet.getAuthor());
        assertNull(changeSet.getDescription());
        assertNotNull(changeSet.getCreated());
    }

    @Test
    public void testThatDoubleArgumentConstructorWithNullArgumentIsAllowed() {
        ChangeSet changeSet = new ChangeSet("test", "Michael de Jong", null);

        assertEquals("Michael de Jong", changeSet.getAuthor());
        assertNull(changeSet.getDescription());
        assertNotNull(changeSet.getCreated());
    }

    @Test
    public void testThatSingleArgumentConstructorWithEmptyStringArgumentThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ChangeSet("test", ""));
    }

    @Test
    public void testThatSingleArgumentConstructorWithNullArgumentThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ChangeSet("test", null));
    }

    @Test
    public void testThatTripleArgumentConstructorWithNullArgumentThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ChangeSet("test", "Michael de Jong", null, null));
    }

}
