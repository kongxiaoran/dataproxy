/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.plugin.database.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.plugin.database.config.*;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;
import org.secretflow.v1alpha1.common.Common;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;


@Slf4j
public class DatabaseDoGetContext {

    // DECIMAL type aliases
    private static final Set<String> DECIMAL_TYPE_NAMES = Set.of("DECIMAL", "NUMERIC", "NUMBER", "DEC");
    
    // Time types that require precision information
    private static final Set<String> TIME_TYPE_NAMES = Set.of("DATETIME", "TIMESTAMP", "TIME");
    
    // Default precision and scale for DECIMAL type
    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 10;

    /**
     * SQL integer supplier functional interface for simplified exception handling.
     */
    @FunctionalInterface
    private interface SqlIntSupplier {
        int getAsInt() throws SQLException;
    }

    /**
     * Column information inner class for unified handling of column metadata from different sources.
     */
    private static class ColumnInfo {
        final String name;
        final String type;
        final int precision;
        final int scale;

        ColumnInfo(String name, String type, int precision, int scale) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    private final DatabaseCommandConfig<?> dbCommandConfig;

    @Getter
    private Schema schema;

    @Getter
    private ResultSet resultSet;

    private Statement queryStmt;
    private Connection conn;
    @Getter
    private DatabaseMetaData databaseMetaData;

    @Getter
    private String tableName;

    private final Map<byte[], ParamWrapper> ticketWrapperMap = new ConcurrentHashMap<>();
    private final ReadWriteLock ticketWrapperMapRwLock = new ReentrantReadWriteLock();

    private final Function<DatabaseConnectConfig, Connection> initDatabaseFunc;

    @FunctionalInterface
    public interface BuildQuerySqlFunc<T, U, V, R> {
        R apply(T t, U u, V v);
    }
    private final BuildQuerySqlFunc<String, List<String>, String, String> buildQuerySqlFunc;

    private final Function<String, ArrowType> jdbcType2ArrowType;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();


    public DatabaseDoGetContext(DatabaseCommandConfig<?> config, Function<DatabaseConnectConfig, Connection> initDatabaseFunc, BuildQuerySqlFunc<String, List<String>, String, String> buildQuerySqlFunc, Function<String, ArrowType> jdbcType2ArrowType) {
        this.dbCommandConfig = config;
        this.initDatabaseFunc = initDatabaseFunc;
        this.buildQuerySqlFunc = buildQuerySqlFunc;
        this.jdbcType2ArrowType = jdbcType2ArrowType;
        prepare();
    }

    public List<TaskConfig> getTaskConfigs() {
        return Collections.singletonList(new TaskConfig(this, 0));
    }

    private void prepare() {
        DatabaseConnectConfig dbConnectConfig = dbCommandConfig.getDbConnectConfig();

        String querySql;
        Connection localConn = null;
        
        try {
            localConn = this.initDatabaseFunc.apply(dbConnectConfig);
            this.conn = localConn;
            
            if (dbCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
                querySql = scqlReadJobConfig.getCommandConfig();
            } else if (dbCommandConfig instanceof DatabaseTableQueryConfig dbTableQueryConfig) {
                DatabaseTableConfig tableConfig = dbTableQueryConfig.getCommandConfig();
                this.tableName = tableConfig.tableName();
                querySql = this.buildQuerySqlFunc.apply(this.tableName, 
                        tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), 
                        tableConfig.partition());
                this.schema = dbCommandConfig.getResultSchema();
            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, 
                        "Unsupported read parameter type: " + dbCommandConfig.getClass());
            }
            
            this.executeSqlTaskAndHandleResult(localConn, this.tableName, querySql);
        } catch (Exception e) {
            // Clean up created connection if prepare fails
            if (localConn != null) {
                try {
                    localConn.close();
                } catch (SQLException closeException) {
                    log.warn("Failed to close connection after prepare error", closeException);
                }
            }
            throw e;
        }
    }

    private void executeSqlTaskAndHandleResult(Connection connection, String tableName, String querySql) {
        log.debug("Executing SQL on table {}: {}", 
                tableName != null ? tableName : "N/A",
                querySql.length() > 200 ? querySql.substring(0, 200) + "..." : querySql);

        Throwable error = null;
        readWriteLock.writeLock().lock();
        try {
            this.databaseMetaData = connection.getMetaData();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(querySql);
            
            // Assign to instance variables after successful execution
            this.queryStmt = stmt;
            this.resultSet = rs;
            
            // SQL queries get column information from ResultSetMetaData, table queries from table metadata
            if (dbCommandConfig.getDbTypeEnum() == DatabaseTypeEnum.SQL) {
                this.initArrowSchemaFromResultSet(rs);
            } else {
                this.initArrowSchemaFromColumns(connection.getMetaData(), tableName);
            }
        } catch (Exception e) {
            error = e;
            // Clean up resources created in this execution (unassigned ones handled by GC, assigned ones by close())
            closeResourcesQuietly(this.resultSet, this.queryStmt);
            this.resultSet = null;
            this.queryStmt = null;
            
            String msg = e instanceof SQLException ? e.getMessage() : "database execute sql error";
            log.error("SQL execution failed on table {}: {}", tableName != null ? tableName : "N/A", e.getMessage(), e);
            throw DataproxyException.of(DataproxyErrorCode.DATABASE_ERROR, msg, e);
        } finally {
            readWriteLock.writeLock().unlock();
            loadLazyConfig(error);
        }
    }
    
    /**
     * Silently close resources (for cleanup in exception handling).
     */
    private void closeResourcesQuietly(ResultSet rs, Statement stmt) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Failed to close ResultSet during error handling", e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Failed to close Statement during error handling", e);
            }
        }
    }

    public void close() {
        SQLException firstException = null;
        
        // Close resources in reverse order
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Failed to close ResultSet: {}", e.getMessage(), e);
                firstException = e;
            }
        }
        
        if (queryStmt != null) {
            try {
                queryStmt.close();
            } catch (SQLException e) {
                log.error("Failed to close Statement: {}", e.getMessage(), e);
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("Failed to close Connection: {}", e.getMessage(), e);
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        
        if (firstException != null) {
            throw new RuntimeException("Error closing database resources", firstException);
        }
    }

    /**
     * Initialize Arrow Schema from DatabaseMetaData.
     * Used for table query scenarios to get complete column information of the table.
     *
     * @param metaData Database metadata
     * @param tableName Table name
     * @throws SQLException SQL exception
     */
    private void initArrowSchemaFromColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        String schemaName = dbCommandConfig.getDbConnectConfig().database();
        log.debug("Querying column information: schema={}, catalog=null, tableName={}", schemaName, tableName);
        
        List<ColumnInfo> columnInfos = new ArrayList<>();
        try (ResultSet columns = metaData.getColumns(null, schemaName, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                
                // Safely get precision and scale information
                int precision = safeGetInt(() -> columns.getInt("COLUMN_SIZE"), columnName, "COLUMN_SIZE", 0);
                int scale = safeGetInt(() -> columns.getInt("DECIMAL_DIGITS"), columnName, "DECIMAL_DIGITS", -1);
                
                columnInfos.add(new ColumnInfo(columnName, columnType, precision, scale));
            }
        }
        
        schema = buildSchemaFromColumnInfos(columnInfos);
        log.debug("Built schema with {} columns for table {}", columnInfos.size(), tableName);
    }

    /**
     * Initialize Arrow Schema from ResultSetMetaData.
     * Used for SQL query scenarios to get actual column information of query results.
     *
     * @param resultSet Query result set
     * @throws SQLException SQL exception
     */
    private void initArrowSchemaFromResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<ColumnInfo> columnInfos = new ArrayList<>(columnCount);

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            String columnType = metaData.getColumnTypeName(i);
            
            final int index = i;
            // Safely get precision and scale information
            int precision = safeGetInt(() -> metaData.getPrecision(index), columnName, "precision", 0);
            int scale = safeGetInt(() -> metaData.getScale(index), columnName, "scale", -1);
            
            columnInfos.add(new ColumnInfo(columnName, columnType, precision, scale));
        }

        schema = buildSchemaFromColumnInfos(columnInfos);
        log.debug("Built schema with {} columns from SQL query result", columnCount);
    }

    /**
     * Build Arrow Schema from column information list (unified Schema building logic).
     *
     * @param columnInfos Column information list
     * @return Arrow Schema
     */
    private Schema buildSchemaFromColumnInfos(List<ColumnInfo> columnInfos) {
        List<Field> fields = new ArrayList<>(columnInfos.size());
        
        for (ColumnInfo info : columnInfos) {
            ArrowType arrowType = determineArrowType(
                info.type,
                () -> info.precision,
                () -> info.scale,
                info.name
            );
            fields.add(new Field(info.name, FieldType.nullable(arrowType), null));
        }
        
        return new Schema(fields);
    }

    /**
     * Safely get integer value, catch SQLException and return default value.
     *
     * @param supplier SQL integer supplier
     * @param columnName Column name (for logging)
     * @param attributeName Attribute name (for logging)
     * @param defaultValue Default value
     * @return Retrieved integer value or default value
     */
    private int safeGetInt(SqlIntSupplier supplier, String columnName, String attributeName, int defaultValue) {
        try {
            return supplier.getAsInt();
        } catch (SQLException e) {
            log.warn("Failed to get {} for column {}: {}", attributeName, columnName, e.getMessage());
            return defaultValue;
        }
    }

    /**
     * Determine Arrow type based on JDBC type name.
     * 
     * @param columnType JDBC type name (e.g., "TIMESTAMP(6)", "DECIMAL", "TIME")
     * @param precisionSupplier Precision supplier (for DECIMAL type)
     * @param scaleSupplier Scale/precision supplier (for time type precision in DECIMAL type)
     * @param columnName Column name (for logging)
     * @return Arrow type
     */
    private ArrowType determineArrowType(String columnType, 
                                         java.util.function.Supplier<Integer> precisionSupplier,
                                         java.util.function.Supplier<Integer> scaleSupplier,
                                         String columnName) {
        // Check if it's DECIMAL type
        if (isDecimalType(columnType)) {
            int precision = precisionSupplier.get();
            int scale = scaleSupplier.get();
            // If retrieval fails (returns 0 or negative), use default value
            if (precision <= 0) {
                precision = DEFAULT_DECIMAL_PRECISION;
            }
            if (scale < 0) {
                scale = DEFAULT_DECIMAL_SCALE;
            }
            return new ArrowType.Decimal(precision, scale, 128);
        }
        
        // For time types, try to construct type name with precision from precision information
        String typeNameWithPrecision = addPrecisionToTypeName(columnType, scaleSupplier, columnName);
        return this.jdbcType2ArrowType.apply(typeNameWithPrecision);
    }

    /**
     * Extract base type name (remove precision information).
     * Example: DECIMAL(38,10) -> DECIMAL, TIMESTAMP(6) -> TIMESTAMP
     * 
     * @param columnType JDBC type name
     * @return Base type name (uppercase, precision information removed)
     */
    private String extractBaseTypeName(String columnType) {
        if (columnType == null) {
            return null;
        }
        return columnType.toUpperCase().replaceAll("\\([^)]*\\)", "").trim();
    }

    /**
     * Check if it's DECIMAL type (including aliases).
     * Supports type names with precision information, such as "DECIMAL(38,10)", "NUMERIC(20,5)", etc.
     * 
     * @param columnType JDBC type name
     * @return Whether it's DECIMAL type
     */
    private boolean isDecimalType(String columnType) {
        if (columnType == null) {
            return false;
        }
        String baseType = extractBaseTypeName(columnType);
        return DECIMAL_TYPE_NAMES.contains(baseType);
    }

    /**
     * Add precision information to time type (if TYPE_NAME doesn't contain precision but DECIMAL_DIGITS has value).
     * 
     * @param columnType JDBC type name
     * @param scaleSupplier Precision supplier (DECIMAL_DIGITS)
     * @param columnName Column name (for logging)
     * @return Type name with precision (if applicable)
     */
    private String addPrecisionToTypeName(String columnType,
                                          java.util.function.Supplier<Integer> scaleSupplier,
                                          String columnName) {
        // If type name already contains precision information, return directly
        if (columnType != null && columnType.contains("(")) {
            return columnType;
        }
        
        // Extract base type name (remove possible precision information)
        String baseType = extractBaseTypeName(columnType);
        
        // Check if it's a time type that requires precision information
        if (!isTimeType(baseType)) {
            return columnType;
        }
        
        // Try to get precision from DECIMAL_DIGITS
        int decimalDigits = scaleSupplier.get();
        if (decimalDigits >= 0) {
            String typeNameWithPrecision = baseType + "(" + decimalDigits + ")";
            log.debug("Constructed type name with precision: {} -> {} for column {}", 
                    columnType, typeNameWithPrecision, columnName);
            return typeNameWithPrecision;
        }
        
        return columnType;
    }

    /**
     * Check if it's a time type that requires precision information.
     * 
     * @param baseType Base type name (uppercase, precision information removed)
     * @return Whether it's a time type
     */
    private boolean isTimeType(String baseType) {
        return TIME_TYPE_NAMES.contains(baseType);
    }


    private void loadLazyConfig(Throwable throwable) {
        ticketWrapperMapRwLock.writeLock().lock();
        try {
            if (ticketWrapperMap.isEmpty()) {
                return;
            }
            List<TaskConfig> taskConfigs = getTaskConfigs();
            if (taskConfigs.isEmpty()) {
                throw new IllegalArgumentException("#getTaskConfigs is empty");
            }

            log.debug("Loading lazy config: taskConfigs size={}, ticketWrapperMap size={}", 
                    taskConfigs.size(), ticketWrapperMap.size());

            int index = 0;
            ParamWrapper paramWrapper;
            for (Map.Entry<byte[], ParamWrapper> entry : ticketWrapperMap.entrySet()) {
                paramWrapper = entry.getValue();

                if (index < taskConfigs.size()) {
                    TaskConfig taskConfig = taskConfigs.get(index);
                    taskConfig.setError(throwable);
                    log.debug("Loading lazy taskConfig at index {}", index);
                    paramWrapper.setParamIfAbsent(taskConfig);
                } else {
                    log.debug("Setting default TaskConfig for remaining ticket at index {}", index);
                    TaskConfig taskConfig = new TaskConfig(this, 0);
                    taskConfig.setError(throwable);
                    paramWrapper.setParamIfAbsent(taskConfig);
                }
                index++;
            }
        } finally {
            ticketWrapperMapRwLock.writeLock().unlock();
        }
    }

}
