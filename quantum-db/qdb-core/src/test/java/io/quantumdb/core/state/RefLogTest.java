package io.quantumdb.core.state;

import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static io.quantumdb.core.utils.RandomHasher.generateHash;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.SyncRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;

public class RefLogTest {

    private Catalog catalog;
    private RefLog  refLog;
    private Version version;

    @BeforeEach
    public void setUp() {
        this.catalog = new Catalog("test-db");
        catalog.addTable(new Table("users").addColumn(new Column("id", bigint(), IDENTITY))
                                           .addColumn(new Column("name", varchar(255))));

        this.version = new Version(generateHash(), null);
        this.refLog = RefLog.init(catalog, version);
    }

    @Test
    public void testAddingNewTable() {
        String refId = generateHash();
        refLog.addTable("transactions", refId, version,
                        Lists.newArrayList(new ColumnRef("id"), new ColumnRef("sender_id"),
                                           new ColumnRef("receiver_id"), new ColumnRef("amount")));

        TableRef tableRef = refLog.getTableRef(version, "transactions");
        assertEquals("transactions", tableRef.getName());
        assertEquals(refId, tableRef.getRefId());
        assertEquals(ImmutableSet.of("id", "sender_id", "receiver_id", "amount"), tableRef.getColumns().keySet());
        assertEquals(Sets.newHashSet(version), tableRef.getVersions());
    }

    @Test
    public void testAddingNewTableBasedOnOtherTable() {
        String refId = generateHash();
        TableRef users = refLog.getTableRef(version, "users");
        ImmutableMap<String, ColumnRef> userColumns = users.getColumns();
        Version nextVersion = new Version(generateHash(), version);

        ColumnRef idColumn = new ColumnRef("id", Sets.newHashSet(userColumns.get("id")));
        ColumnRef nameColumn = new ColumnRef("name", Sets.newHashSet(userColumns.get("name")));
        refLog.addTable("users_v2", refId, nextVersion, Lists.newArrayList(idColumn, nameColumn));

        TableRef usersV2 = refLog.getTableRef(nextVersion, "users_v2");
        assertEquals("users_v2", usersV2.getName());
        assertEquals(refId, usersV2.getRefId());
        assertEquals(ImmutableMap.of("id", idColumn, "name", nameColumn), usersV2.getColumns());
        assertEquals(Sets.newHashSet(nextVersion), usersV2.getVersions());
    }

    @Test
    public void testAddingSync() {
        String refId = generateHash();
        Version nextVersion = new Version(generateHash(), version);
        refLog.fork(nextVersion);
        TableRef oldRef = refLog.getTableRef(version, "users");
        TableRef newRef = refLog.replaceTable(nextVersion, "users", "users", refId);

        Map<ColumnRef, ColumnRef> columnMapping = oldRef.getColumns()
                                                        .entrySet()
                                                        .stream()
                                                        .collect(Collectors.toMap(Entry::getValue,
                                                                                  entry -> newRef.getColumns()
                                                                                                 .get(entry.getKey())));

        SyncRef syncRef = refLog.addSync(generateHash(), generateHash(), columnMapping);
        assertEquals(oldRef, syncRef.getSource());
        assertEquals(newRef, syncRef.getTarget());
    }

    @Test
    public void testAddingTableWithEmptyIdThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> refLog.addTable("transactions", "", version, Lists.newArrayList()));
    }

    @Test
    public void testAddingTableWithEmptyNameThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> refLog.addTable("", generateHash(), version, Lists.newArrayList()));
    }

    @Test
    public void testAddingTableWithExistingNameAndVersionCombinationThrowsException() {
        assertThrows(IllegalStateException.class,
                     () -> refLog.addTable("users", generateHash(), version, Lists.newArrayList()));
    }

    @Test
    public void testAddingTableWithNullIdThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> refLog.addTable("transactions", null, version, Lists.newArrayList()));
    }

    @Test
    public void testAddingTableWithNullNameThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> refLog.addTable(null, generateHash(), version, Lists.newArrayList()));
    }

    @Test
    public void testAddingTableWithNullVersionThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> refLog.addTable("transactions", generateHash(), null, Lists.newArrayList()));
    }

    @Test
    public void testDroppingEntireTableRef() {
        TableRef tableRef = refLog.getTableRef(version, "users");
        refLog.dropTable(tableRef);

        assertTrue(refLog.getTableRefs().isEmpty());
    }

    @Test
    public void testDroppingEntireTableRefThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> refLog.dropTable(null));
    }

    @Test
    public void testEmptyConstructor() {
        new RefLog();
    }

    @Test
    public void testForkingThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> refLog.fork(null));
    }

    @Test
    public void testForkingThrowsExceptionOnRootNode() {
        assertThrows(IllegalArgumentException.class, () -> refLog.fork(version));
    }

    @Test
    public void testForkingThrowsExceptionOnUnknownNode() {
        assertThrows(IllegalArgumentException.class, () -> {
            Version secondVersion = new Version(generateHash(), version);
            Version thirdVersion = new Version(generateHash(), secondVersion);
            refLog.fork(thirdVersion);
        });
    }

    @Test
    public void testForkingVersion() {
        Version nextVersion = new Version(generateHash(), version);
        refLog.fork(nextVersion);

        List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

        assertEquals(1, refs.size());
        TableRef ref = refs.get(0);
        assertEquals("users", ref.getName());
        assertEquals("users", ref.getRefId());
        assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
        assertEquals(Sets.newHashSet(version, nextVersion), ref.getVersions());
    }

    @Test
    public void testReplacingTable() {
        String refId = generateHash();
        refLog.replaceTable(version, "users", "users", refId);

        List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

        assertEquals(1, refs.size());
        TableRef ref = refs.get(0);
        assertEquals("users", ref.getName());
        assertEquals(refId, ref.getRefId());
        assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
        assertEquals(Sets.newHashSet(version), ref.getVersions());
    }

    @Test
    public void testReplacingTableInNextVersion() {
        String refId = generateHash();
        Version nextVersion = new Version(generateHash(), version);
        refLog.fork(nextVersion);
        refLog.replaceTable(nextVersion, "users", "users", refId);

        Map<Version, TableRef> refs = refLog.getTableRefs()
                                            .stream()
                                            .collect(Collectors.toMap(ref -> ref.getVersions()
                                                                                .stream()
                                                                                .findFirst()
                                                                                .get(),
                                                                      Function.identity()));

        assertEquals(2, refs.size());

        TableRef ref1 = refs.get(version);
        assertEquals("users", ref1.getName());
        assertEquals("users", ref1.getRefId());
        assertEquals(ImmutableSet.of("id", "name"), ref1.getColumns().keySet());
        assertEquals(Sets.newHashSet(version), ref1.getVersions());

        TableRef ref2 = refs.get(nextVersion);
        assertEquals("users", ref2.getName());
        assertEquals(refId, ref2.getRefId());
        assertEquals(ImmutableSet.of("id", "name"), ref2.getColumns().keySet());
        assertEquals(Sets.newHashSet(nextVersion), ref2.getVersions());

        assertEquals(ImmutableSet.of(ref2.getColumns().get("id")), ref1.getColumns().get("id").getBasisFor());
        assertEquals(ImmutableSet.of(ref1.getColumns().get("id")), ref2.getColumns().get("id").getBasedOn());
        assertEquals(ImmutableSet.of(ref2.getColumns().get("name")), ref1.getColumns().get("name").getBasisFor());
        assertEquals(ImmutableSet.of(ref1.getColumns().get("name")), ref2.getColumns().get("name").getBasedOn());
    }

    @Test
    public void testReplacingTableThrowsExceptionOnEmptyRefId() {
        assertThrows(IllegalArgumentException.class, () -> refLog.replaceTable(version, "users", "users", ""));
    }

    @Test
    public void testReplacingTableThrowsExceptionOnEmptySourceTableName() {
        assertThrows(IllegalArgumentException.class, () -> {
            String refId = generateHash();
            refLog.replaceTable(version, "", "users", refId);
        });
    }

    @Test
    public void testReplacingTableThrowsExceptionOnEmptyTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> {
            String refId = generateHash();
            refLog.replaceTable(version, "users", "", refId);
        });
    }

    @Test
    public void testReplacingTableThrowsExceptionOnNullRefId() {
        assertThrows(IllegalArgumentException.class, () -> refLog.replaceTable(version, "users", "users", null));
    }

    @Test
    public void testReplacingTableThrowsExceptionOnNullSourceTableName() {
        assertThrows(IllegalArgumentException.class, () -> {
            String refId = generateHash();
            refLog.replaceTable(version, null, "users", refId);
        });
    }

    @Test
    public void testReplacingTableThrowsExceptionOnNullTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> {
            String refId = generateHash();
            refLog.replaceTable(version, "users", null, refId);
        });
    }

    @Test
    public void testReplacingTableThrowsExceptionOnNullVersion() {
        assertThrows(IllegalArgumentException.class, () -> {
            String refId = generateHash();
            refLog.replaceTable(null, "users", "users", refId);
        });
    }

    @Test
    public void testStaticConstructor() {
        List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

        assertEquals(1, refs.size());
        TableRef ref = refs.get(0);
        assertEquals("users", ref.getName());
        assertEquals("users", ref.getRefId());
        assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
        assertEquals(Sets.newHashSet(version), ref.getVersions());
    }

    @Test
    public void testStaticConstructorThrowsExceptionOnNonRootVersion() {
        assertThrows(IllegalArgumentException.class, () -> {
            Version nextVersion = new Version(generateHash(), version);
            RefLog.init(catalog, nextVersion);
        });
    }

    @Test
    public void testStaticConstructorThrowsExceptionOnNullCatalog() {
        assertThrows(IllegalArgumentException.class, () -> RefLog.init(null, version));
    }

    @Test
    public void testStaticConstructorThrowsExceptionOnNullVersion() {
        assertThrows(IllegalArgumentException.class, () -> RefLog.init(catalog, null));
    }

}
