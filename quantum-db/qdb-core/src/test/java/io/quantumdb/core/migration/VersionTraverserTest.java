package io.quantumdb.core.migration;

import static io.quantumdb.core.migration.VersionTraverser.findPath;
import static io.quantumdb.core.migration.VersionTraverser.getDirection;
import static io.quantumdb.core.migration.VersionTraverser.getFirst;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.quantumdb.core.migration.VersionTraverser.Direction;
import io.quantumdb.core.versioning.Version;

public class VersionTraverserTest {

    private Version v1;
    private Version v2;
    private Version v3;
    private Version v4;

    @BeforeEach
    public void setUp() {
        this.v1 = new Version("1", null);
        this.v2 = new Version("2", v1);
        this.v3 = new Version("3", v2);
        this.v4 = new Version("4", v3);
    }

    @Test
    public void testFindingPathInDisconnectedChangelogsFails() {
        Version vA = new Version("A", null);
        Version vB = new Version("B", vA);
        Version vC = new Version("C", vB);
        @SuppressWarnings("unused")
        Version vD = new Version("D", vC);

        Optional<List<Version>> path = findPath(v1, vA);
        assertEquals(Optional.empty(), path);
    }

    @Test
    public void testFindingPathWithNullInputForOrigin() {
        assertThrows(IllegalArgumentException.class, () -> findPath(null, v4));
    }

    @Test
    public void testFindingPathWithNullInputForTarget() {
        assertThrows(IllegalArgumentException.class, () -> findPath(v1, null));
    }

    @Test
    public void testFindingReversePath() {
        List<Version> path = findPath(v4, v1).get();
        assertEquals(Lists.newArrayList(v4, v3, v2, v1), path);
    }

    @Test
    public void testFindPath() {
        List<Version> path = findPath(v1, v4).get();
        assertEquals(Lists.newArrayList(v1, v2, v3, v4), path);
    }

    @Test
    public void testGetDirectionBetweenRootAndV2() {
        Direction direction = getDirection(v1, v2);
        assertEquals(direction, Direction.FORWARDS);
    }

    @Test
    public void testGetDirectionBetweenV2AndV3() {
        Direction direction = getDirection(v3, v2);
        assertEquals(direction, Direction.BACKWARDS);
    }

    @Test
    public void testGetDirectionWithDuplicateInput() {
        assertThrows(IllegalArgumentException.class, () -> getDirection(v2, v2));
    }

    @Test
    public void testGetDirectionWithNullAsFromInput() {
        assertThrows(IllegalArgumentException.class, () -> getDirection(null, v2));
    }

    @Test
    public void testGetDirectionWithNullAsToInput() {
        assertThrows(IllegalArgumentException.class, () -> getDirection(v2, null));
    }

    @Test
    public void testGetFirstExcludingRoot() {
        Version first = getFirst(Sets.newHashSet(v2, v3));
        assertEquals(v2, first);
    }

    @Test
    public void testGetFirstIncludingRoot() {
        Version first = getFirst(Sets.newHashSet(v1, v2));
        assertEquals(v1, first);
    }

    @Test
    public void testGetFirstOnEmptySet() {
        Version first = getFirst(Sets.newHashSet());
        assertNull(first);
    }

    @Test
    public void testGetFirstOnGap() {
        Version first = getFirst(Sets.newHashSet(v2, v4));
        assertEquals(v2, first);
    }

    public void testGetFirstOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> getFirst(null));
    }

}
