/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.String;
import sandbox.java.sql.Connection;
import sandbox.java.sql.DatabaseMetaData;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.RowIdLifetime;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class DatabaseMetadataWrapper implements DatabaseMetaData {

    private final java.sql.DatabaseMetaData wrapped;

    public DatabaseMetadataWrapper(java.sql.DatabaseMetaData wrapped) {
        this.wrapped = wrapped;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        try {
            return wrapped.allProceduresAreCallable();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean allTablesAreSelectable() throws SQLException {
        try {
            return wrapped.allTablesAreSelectable();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        try {
            return wrapped.autoCommitFailureClosesAllResultSets();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        try {
            return wrapped.dataDefinitionCausesTransactionCommit();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        try {
            return wrapped.dataDefinitionIgnoredInTransactions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        try {
            return wrapped.deletesAreDetected(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        try {
            return wrapped.doesMaxRowSizeIncludeBlobs();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        try {
            return wrapped.generatedKeyAlwaysReturned();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        try {
            return new ResultSetWrapper(
                    wrapped.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          boolean nullable) throws SQLException {
        try {
            return wrapped.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getCatalogs() throws SQLException {
        try {
            return wrapped.getCatalogs();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getCatalogSeparator() throws SQLException {
        try {
            return wrapped.getCatalogSeparator();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getCatalogTerm() throws SQLException {
        try {
            return wrapped.getCatalogTerm();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        try {
            return wrapped.getClientInfoProperties();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        try {
            return wrapped.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        try {
            return wrapped.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Connection getConnection() throws SQLException {
        try {
            return wrapped.getConnection();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema,
                                       String foreignTable) throws SQLException {
        try {
            return wrapped.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema,
                                             foreignTable);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getDatabaseMajorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMajorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getDatabaseMinorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMinorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getDatabaseProductName() throws SQLException {
        try {
            return wrapped.getDatabaseProductName();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getDatabaseProductVersion() throws SQLException {
        try {
            return wrapped.getDatabaseProductVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        try {
            return wrapped.getDefaultTransactionIsolation();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getDriverMajorVersion() {
        try {
            return wrapped.getDriverMajorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getDriverMinorVersion() {
        try {
            return wrapped.getDriverMinorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getDriverName() throws SQLException {
        try {
            return wrapped.getDriverName();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getDriverVersion() throws SQLException {
        try {
            return wrapped.getDriverVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getExportedKeys(catalog, schema, table);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getExtraNameCharacters() throws SQLException {
        try {
            return wrapped.getExtraNameCharacters();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        try {
            return wrapped.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getFunctions(String catalog, String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        try {
            return wrapped.getFunctions(catalog, schemaPattern, functionNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getIdentifierQuoteString() throws SQLException {
        try {
            return wrapped.getIdentifierQuoteString();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getImportedKeys(catalog, schema, table);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {
        try {
            return wrapped.getIndexInfo(catalog, schema, table, unique, approximate);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getJDBCMajorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMajorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getJDBCMinorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMinorVersion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxBinaryLiteralLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxCatalogNameLength() throws SQLException {
        try {
            return wrapped.getMaxCatalogNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxCharLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxCharLiteralLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnNameLength() throws SQLException {
        try {
            return wrapped.getMaxColumnNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInGroupBy();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnsInIndex() throws SQLException {
        try {
            return wrapped.getMaxColumnsInIndex();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInOrderBy();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnsInSelect() throws SQLException {
        try {
            return wrapped.getMaxColumnsInSelect();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxColumnsInTable() throws SQLException {
        try {
            return wrapped.getMaxColumnsInTable();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxConnections() throws SQLException {
        try {
            return wrapped.getMaxConnections();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxCursorNameLength() throws SQLException {
        try {
            return wrapped.getMaxCursorNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxIndexLength() throws SQLException {
        try {
            return wrapped.getMaxIndexLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long getMaxLogicalLobSize() throws SQLException {
        try {
            return wrapped.getMaxLogicalLobSize();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxProcedureNameLength() throws SQLException {
        try {
            return wrapped.getMaxProcedureNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxRowSize() throws SQLException {
        try {
            return wrapped.getMaxRowSize();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxSchemaNameLength() throws SQLException {
        try {
            return wrapped.getMaxSchemaNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxStatementLength() throws SQLException {
        try {
            return wrapped.getMaxStatementLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxStatements() throws SQLException {
        try {
            return wrapped.getMaxStatements();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxTableNameLength() throws SQLException {
        try {
            return wrapped.getMaxTableNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxTablesInSelect() throws SQLException {
        try {
            return wrapped.getMaxTablesInSelect();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxUserNameLength() throws SQLException {
        try {
            return wrapped.getMaxUserNameLength();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getNumericFunctions() throws SQLException {
        try {
            return wrapped.getNumericFunctions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getPrimaryKeys(catalog, schema, table);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        try {
            return wrapped.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        try {
            return wrapped.getProcedures(catalog, schemaPattern, procedureNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getProcedureTerm() throws SQLException {
        try {
            return wrapped.getProcedureTerm();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        try {
            return wrapped.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getResultSetHoldability() throws SQLException {
        try {
            return wrapped.getResultSetHoldability();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        try {
            return wrapped.getRowIdLifetime();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getSchemas() throws SQLException {
        try {
            return wrapped.getSchemas();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        try {
            return wrapped.getSchemas(catalog, schemaPattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSchemaTerm() throws SQLException {
        try {
            return wrapped.getSchemaTerm();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSearchStringEscape() throws SQLException {
        try {
            return wrapped.getSearchStringEscape();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSQLKeywords() throws SQLException {
        try {
            return wrapped.getSQLKeywords();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getSQLStateType() throws SQLException {
        try {
            return wrapped.getSQLStateType();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getStringFunctions() throws SQLException {
        try {
            return wrapped.getStringFunctions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            return wrapped.getSuperTables(catalog, schemaPattern, tableNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        try {
            return wrapped.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSystemFunctions() throws SQLException {
        try {
            return wrapped.getSystemFunctions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        try {
            return wrapped.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        try {
            return wrapped.getTables(catalog, schemaPattern, tableNamePattern, types);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getTableTypes() throws SQLException {
        try {
            return wrapped.getTableTypes();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getTimeDateFunctions() throws SQLException {
        try {
            return wrapped.getTimeDateFunctions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getTypeInfo() throws SQLException {
        try {
            return wrapped.getTypeInfo();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                             int[] types) throws SQLException {
        try {
            return wrapped.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getURL() throws SQLException {
        try {
            return wrapped.getURL();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getUserName() throws SQLException {
        try {
            return wrapped.getUserName();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getVersionColumns(catalog, schema, table);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        try {
            return wrapped.insertsAreDetected(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isCatalogAtStart() throws SQLException {
        try {
            return wrapped.isCatalogAtStart();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isReadOnly() throws SQLException {
        try {
            return wrapped.isReadOnly();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        try {
            return wrapped.locatorsUpdateCopy();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        try {
            return wrapped.nullPlusNonNullIsNull();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtEnd();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtStart();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        try {
            return wrapped.nullsAreSortedHigh();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean nullsAreSortedLow() throws SQLException {
        try {
            return wrapped.nullsAreSortedLow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersDeletesAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersInsertsAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersUpdatesAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownDeletesAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownInsertsAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownUpdatesAreVisible(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseQuotedIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseQuotedIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseQuotedIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithAddColumn();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithDropColumn();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92EntryLevelSQL();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92FullSQL();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92IntermediateSQL();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsBatchUpdates() throws SQLException {
        try {
            return wrapped.supportsBatchUpdates();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsCatalogsInDataManipulation();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInIndexDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInPrivilegeDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsCatalogsInProcedureCalls();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInTableDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsColumnAliasing() throws SQLException {
        try {
            return wrapped.supportsColumnAliasing();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsConvert() throws SQLException {
        try {
            return wrapped.supportsConvert();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        try {
            return wrapped.supportsConvert(fromType, toType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsCoreSQLGrammar();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        try {
            return wrapped.supportsCorrelatedSubqueries();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        try {
            return wrapped.supportsDataDefinitionAndDataManipulationTransactions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        try {
            return wrapped.supportsDataManipulationTransactionsOnly();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsDifferentTableCorrelationNames();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        try {
            return wrapped.supportsExpressionsInOrderBy();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        try {
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
        return wrapped.supportsExtendedSQLGrammar();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        try {
            return wrapped.supportsFullOuterJoins();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        try {
            return wrapped.supportsGetGeneratedKeys();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsGroupBy() throws SQLException {
        try {
            return wrapped.supportsGroupBy();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        try {
            return wrapped.supportsGroupByBeyondSelect();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        try {
            return wrapped.supportsGroupByUnrelated();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        try {
            return wrapped.supportsIntegrityEnhancementFacility();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        try {
            return wrapped.supportsLikeEscapeClause();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        try {
            return wrapped.supportsLimitedOuterJoins();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsMinimumSQLGrammar();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseQuotedIdentifiers();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        try {
            return wrapped.supportsMultipleOpenResults();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        try {
            return wrapped.supportsMultipleResultSets();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        try {
            return wrapped.supportsMultipleTransactions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsNamedParameters() throws SQLException {
        try {
            return wrapped.supportsNamedParameters();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        try {
            return wrapped.supportsNonNullableColumns();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossCommit();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossRollback();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossCommit();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossRollback();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        try {
            return wrapped.supportsOrderByUnrelated();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsOuterJoins() throws SQLException {
        try {
            return wrapped.supportsOuterJoins();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsPositionedDelete() throws SQLException {
        try {
            return wrapped.supportsPositionedDelete();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        try {
            return wrapped.supportsPositionedUpdate();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsRefCursors() throws SQLException {
        try {
            return wrapped.supportsRefCursors();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        try {
            return wrapped.supportsResultSetConcurrency(type, concurrency);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        try {
            return wrapped.supportsResultSetHoldability(holdability);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        try {
            return wrapped.supportsResultSetType(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSavepoints() throws SQLException {
        try {
            return wrapped.supportsSavepoints();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsSchemasInDataManipulation();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInIndexDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInPrivilegeDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsSchemasInProcedureCalls();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInTableDefinitions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        try {
            return wrapped.supportsSelectForUpdate();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSharding() throws SQLException {
        try {
            return wrapped.supportsSharding();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsStatementPooling() throws SQLException {
        try {
            return wrapped.supportsStatementPooling();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        try {
            return wrapped.supportsStoredFunctionsUsingCallSyntax();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsStoredProcedures() throws SQLException {
        try {
            return wrapped.supportsStoredProcedures();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInComparisons();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInExists();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInIns();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInQuantifieds();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsTableCorrelationNames();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        try {
            return wrapped.supportsTransactionIsolationLevel(level);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsTransactions() throws SQLException {
        try {
            return wrapped.supportsTransactions();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsUnion() throws SQLException {
        try {
            return wrapped.supportsUnion();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean supportsUnionAll() throws SQLException {
        try {
            return wrapped.supportsUnionAll();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        try {
            return wrapped.updatesAreDetected(type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        try {
            return wrapped.usesLocalFilePerTable();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean usesLocalFiles() throws SQLException {
        try {
            return wrapped.usesLocalFiles();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
