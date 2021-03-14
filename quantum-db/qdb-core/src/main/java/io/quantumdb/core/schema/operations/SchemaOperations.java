
package io.quantumdb.core.schema.operations;

import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;

public class SchemaOperations {
    public static AddColumn addColumn(String tableName, String columnName, ColumnType type, Column.Hint... hints) {
        return addColumn(tableName, columnName, type, null, hints);
    }

    public static AddColumn addColumn(String tableName, String columnName, ColumnType type, String defaultExpression,
                                      Column.Hint... hints) {
        return new AddColumn(tableName, columnName, type, defaultExpression, hints);
    }

    public static AddForeignKey addForeignKey(String table, String... columns) {
        return new AddForeignKey(table, columns);
    }

    public static AlterColumn alterColumn(String tableName, String columnName) {
        return new AlterColumn(tableName, columnName);
    }

    public static CopyTable copyTable(String sourceTableName, String targetTableName) {
        return new CopyTable(sourceTableName, targetTableName);
    }

    public static CreateIndex createIndex(String table, boolean unique, String... columns) {
        return new CreateIndex(table, unique, columns);
    }

    public static CreateTable createTable(String tableName) {
        return new CreateTable(tableName);
    }

    public static CreateView createView(String viewName) {
        return new CreateView(viewName);
    }

    @Deprecated
    public static DecomposeTable decomposeTable(String tableName) {
        return new DecomposeTable(tableName);
    }

    public static DropColumn dropColumn(String tableName, String columnName) {
        return new DropColumn(tableName, columnName);
    }

    public static DropForeignKey dropForeignKey(String tableName, String foreignKeyName) {
        return new DropForeignKey(tableName, foreignKeyName);
    }

    public static DropIndex dropIndex(String tableName, String... columns) {
        return new DropIndex(tableName, columns);
    }

    public static DropTable dropTable(String tableName) {
        return new DropTable(tableName);
    }

    public static DropView dropView(String viewName) {
        return new DropView(viewName);
    }

    public static DataOperation execute(String query) {
        return new DataOperation(query);
    }

    @Deprecated
    public static JoinTable joinTable(String sourceTable, String alias, String... columns) {
        return new JoinTable(sourceTable, alias, columns);
    }

    @Deprecated
    public static MergeTable mergeTable(String leftTableName, String rightTableName, String targetTableName) {
        return new MergeTable(leftTableName, rightTableName, targetTableName);
    }

    @Deprecated
    public static PartitionTable partitionTable(String tableName) {
        return new PartitionTable(tableName);
    }

    public static RenameTable renameTable(String tableName, String newTableName) {
        return new RenameTable(tableName, newTableName);
    }

    
    private SchemaOperations() {
    }
}
