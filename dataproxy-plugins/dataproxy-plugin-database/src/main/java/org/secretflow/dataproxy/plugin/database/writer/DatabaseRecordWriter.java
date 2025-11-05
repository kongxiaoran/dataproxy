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

package org.secretflow.dataproxy.plugin.database.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.utils.Record;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class DatabaseRecordWriter implements Writer {
    private final DatabaseCommandConfig<?> commandConfig;

    private final DatabaseConnectConfig dbConnectConfig;
    private final DatabaseTableConfig dbTableConfig;
    private final Function<DatabaseConnectConfig, Connection> initFunc;
    private final BiFunction<Connection, String, Boolean> checkTableExists;
    private final int BATCH_NUM = 500;

    @FunctionalInterface
    public interface BuildCreateTableSqlFunc {
        String apply(String tableName, Schema schema, Map<String, String> partitionSpec);
    }
    private final BuildCreateTableSqlFunc buildCreateTableSql;
    @FunctionalInterface
    public interface BuildInsertSqlFunc {
        String apply(String tableName, Schema schema, Map<String,Object> data, Map<String, String> partitionSpec);
    }
    private BuildInsertSqlFunc buildInsertSql;

    @FunctionalInterface
    public interface BuildMultiInsertSqlFunc {
        SqlWithParams apply(String tableName, Schema schema, List<Map<String,Object>> data, Map<String, String> partitionSpec);
    }
    private BuildMultiInsertSqlFunc buildMultiInsertSql;
    private final Map<String, String> partitionSpec;
    private final String tableName;
    private Connection connection;
    private final boolean supportMultiInsert;

    /**
     * Parse partition string to map
     * Format: "key1=value1,key2=value2" or "key=value"
     * Example: "dt=20240101" or "dt=20240101,region=us"
     * 
     * @param partition Partition string (e.g., "dt=20240101")
     * @return Map of partition key-value pairs
     */
    public static Map<String, String> parsePartition(String partition) {
        Map<String, String> partitionMap = new LinkedHashMap<>();
        if (partition == null || partition.trim().isEmpty()) {
            return partitionMap;
        }
        
        String trimmed = partition.trim();
        // Split by comma for multiple partitions
        String[] parts = trimmed.split(",");
        
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) {
                continue;
            }
            
            // Split by '=' to get key and value
            int equalIndex = part.indexOf('=');
            if (equalIndex <= 0 || equalIndex >= part.length() - 1) {
                log.warn("Invalid partition format: {}, expected format: key=value", part);
                continue;
            }
            
            String key = part.substring(0, equalIndex).trim();
            String value = part.substring(equalIndex + 1).trim();
            
            if (key.isEmpty() || value.isEmpty()) {
                log.warn("Invalid partition format: {}, key or value is empty", part);
                continue;
            }
            
            partitionMap.put(key, value);
        }
        
        return partitionMap;
    }

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig,
                                Function<DatabaseConnectConfig, Connection> initFunc,
                                BuildCreateTableSqlFunc buildCreateTableSql,
                                BuildInsertSqlFunc buildInsertSql,
                                BiFunction<Connection, String, Boolean> checkTableExists) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.checkTableExists = checkTableExists;
        this.buildCreateTableSql = buildCreateTableSql;
        this.buildInsertSql = buildInsertSql;
        this.tableName = this.dbTableConfig.tableName();
        this.partitionSpec = parsePartition(this.dbTableConfig.partition());
        supportMultiInsert = false;
        this.prepare();
    }

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig,
                                Function<DatabaseConnectConfig, Connection> initFunc,
                                BuildCreateTableSqlFunc buildCreateTableSql,
                                BuildMultiInsertSqlFunc buildMultiInsertSql,
                                BiFunction<Connection, String, Boolean> checkTableExists) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.checkTableExists = checkTableExists;
        this.buildCreateTableSql = buildCreateTableSql;
        this.buildMultiInsertSql = buildMultiInsertSql;
        this.tableName = this.dbTableConfig.tableName();
        this.partitionSpec = parsePartition(this.dbTableConfig.partition());
        supportMultiInsert = true;
        this.prepare();
    }

    private Connection initDatabaseClient(DatabaseConnectConfig dbConnectConfig) {
        if(dbConnectConfig == null) {
            throw new IllegalArgumentException("connConfig is null");
        }
        return this.initFunc.apply(dbConnectConfig);
    }

    private void prepare() {
        Connection localConnection = null;
        try {
            localConnection = initDatabaseClient(dbConnectConfig);
            this.connection = localConnection;

            // Set autoCommit based on insert mode
            if (supportMultiInsert) {
                // Batch insert mode: use manual transaction, batch commit (better performance)
                try {
                    connection.setAutoCommit(false);
                    log.info("Batch insert mode: autoCommit=false, will commit every {} rows", BATCH_NUM);
                } catch (SQLException e) {
                    log.warn("Failed to set autoCommit=false, using default", e);
                }
            } else {
                // Single insert mode: use auto-commit (simple and direct, each insert committed immediately)
                try {
                    connection.setAutoCommit(true);
                    log.info("Single insert mode: autoCommit=true, each insert auto-committed");
                } catch (SQLException e) {
                    log.warn("Failed to set autoCommit=true, using default", e);
                }
            }

            preProcessing(dbTableConfig.tableName());
        } catch (Exception e) {
            // Clean up created connection if prepare fails
            if (localConnection != null) {
                try {
                    localConnection.close();
                } catch (SQLException closeException) {
                    log.warn("Failed to close connection after prepare error", closeException);
                }
            }
            throw e;
        }
    }

    /**
     * Get field data
     *
     * @param fieldVector field vector
     * @param index       index
     * @return value
     */
    private Object getValue(FieldVector fieldVector, int index) {
        if (fieldVector == null || index < 0) {
            return null;
        }
        
        if (fieldVector.isNull(index)) {
            return null;
        }
        
        ArrowType.ArrowTypeID arrowTypeID = fieldVector.getField().getType().getTypeID();

        switch (arrowTypeID) {
            case Int -> {
                if (fieldVector instanceof IntVector || fieldVector instanceof BigIntVector || fieldVector instanceof SmallIntVector || fieldVector instanceof TinyIntVector) {
                    return fieldVector.getObject(index);
                }
                log.debug("Unexpected Int vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case FloatingPoint -> {
                if (fieldVector instanceof Float4Vector vector) {
                    return vector.get(index);
                } else if (fieldVector instanceof Float8Vector vector) {
                    return vector.get(index);
                }
                log.debug("Unexpected FloatingPoint vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Utf8 -> {
                if (fieldVector instanceof VarCharVector vector) {
                    return new String(vector.get(index), StandardCharsets.UTF_8);
                }
                log.debug("Unexpected Utf8 vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case LargeUtf8 -> {
                if (fieldVector instanceof LargeVarCharVector vector) {
                    return vector.get(index);
                }
                log.debug("Unexpected LargeUtf8 vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Binary -> {
                if (fieldVector instanceof VarBinaryVector vector) {
                    return vector.get(index);
                }
                log.debug("Unexpected Binary vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case LargeBinary -> {
                if (fieldVector instanceof LargeVarBinaryVector vector) {
                    return vector.get(index);
                }
                log.debug("Unexpected LargeBinary vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Decimal -> {
                if (fieldVector instanceof DecimalVector vector) {
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) fieldVector.getField().getType();
                    BigDecimal value = vector.getObject(index);
                    return value;
                }
                log.debug("Unexpected Decimal vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Date -> {
                if (fieldVector instanceof DateDayVector vector) {
                    // DateDayVector stores days since 1970-01-01
                    // Convert to java.sql.Date (date only)
                    int days = vector.get(index);
                    LocalDate date = LocalDate.ofEpochDay(days);
                    return java.sql.Date.valueOf(date);
                } else if (fieldVector instanceof DateMilliVector vector) {
                    // DateMilliVector stores milliseconds since 1970-01-01 00:00:00 UTC
                    // Convert to java.sql.Timestamp (date+time) as database fields are usually TIMESTAMP
                    long millis = vector.get(index);
                    return new java.sql.Timestamp(millis);
                }
                log.debug("Unexpected Date vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Time -> {
                // Time vector getObject() may return LocalTime or LocalDateTime
                // Use getObject() directly instead of manual calculation to avoid timezone and calculation errors
                Object timeObj = fieldVector.getObject(index);
                if (timeObj instanceof LocalTime localTime) {
                    return java.sql.Time.valueOf(localTime);
                } else if (timeObj instanceof LocalDateTime localDateTime) {
                    // If LocalDateTime is returned, extract time part
                    LocalTime localTime = localDateTime.toLocalTime();
                    return java.sql.Time.valueOf(localTime);
                } else if (timeObj instanceof Integer intValue) {
                    // TimeSecVector may return Integer (seconds)
                    int seconds = intValue;
                    int hours = seconds / (60 * 60);
                    int minutes = (seconds % (60 * 60)) / 60;
                    int secs = seconds % 60;
                    LocalTime time = LocalTime.of(hours, minutes, secs);
                    return java.sql.Time.valueOf(time);
                } else if (timeObj instanceof Long longValue) {
                    // TimeMilliVector/TimeMicroVector/TimeNanoVector may return Long
                    // Try to convert to LocalTime
                    if (fieldVector instanceof TimeMilliVector) {
                        // TimeMilliVector: milliseconds since midnight
                        int millis = longValue.intValue();
                        if (millis < 0) {
                            log.debug("TimeMilliVector value is negative: {}, using 0", millis);
                            millis = 0;
                        }
                        int hours = millis / (1000 * 60 * 60);
                        int minutes = (millis % (1000 * 60 * 60)) / (1000 * 60);
                        int seconds = (millis % (1000 * 60)) / 1000;
                        int nanos = (millis % 1000) * 1000000;
                        if (hours >= 24) {
                            hours = hours % 24;
                        }
                        LocalTime time = LocalTime.of(hours, minutes, seconds, nanos);
                        return java.sql.Time.valueOf(time);
                    } else if (fieldVector instanceof TimeMicroVector) {
                        // TimeMicroVector: microseconds since midnight
                        long micros = longValue;
                        if (micros < 0) {
                            log.debug("TimeMicroVector value is negative: {}, using 0", micros);
                            micros = 0;
                        }
                        long totalSeconds = micros / 1_000_000;
                        long nanos = (micros % 1_000_000) * 1000;
                        int hours = (int) (totalSeconds / (60 * 60));
                        int minutes = (int) ((totalSeconds % (60 * 60)) / 60);
                        int seconds = (int) (totalSeconds % 60);
                        if (hours >= 24) {
                            hours = hours % 24;
                        }
                        LocalTime time = LocalTime.of(hours, minutes, seconds, (int) nanos);
                        return java.sql.Time.valueOf(time);
                    } else if (fieldVector instanceof TimeNanoVector) {
                        // TimeNanoVector: nanoseconds since midnight
                        long nanos = longValue;
                        if (nanos < 0) {
                            log.debug("TimeNanoVector value is negative: {}, using 0", nanos);
                            nanos = 0;
                        }
                        long totalSeconds = nanos / 1_000_000_000;
                        int nanoOfSecond = (int) (nanos % 1_000_000_000);
                        int hours = (int) (totalSeconds / (60 * 60));
                        int minutes = (int) ((totalSeconds % (60 * 60)) / 60);
                        int seconds = (int) (totalSeconds % 60);
                        if (hours >= 24) {
                            hours = hours % 24;
                        }
                        LocalTime time = LocalTime.of(hours, minutes, seconds, nanoOfSecond);
                        return java.sql.Time.valueOf(time);
                    } else {
                        log.debug("Time type conversion for Long value not fully supported, using default");
                        return timeObj;
                    }
                }
                log.debug("Time conversion failed, value type: {}", 
                    timeObj != null ? timeObj.getClass().getName() : "null");
                return timeObj;
            }
            case Timestamp -> {
                // Important: TimeStampVector stores UTC time (microseconds/milliseconds since Unix epoch)
                // Cannot use getObject() returned LocalDateTime, because Timestamp.valueOf(LocalDateTime)
                // treats LocalDateTime as local time in system default timezone, causing timezone conversion errors
                // Should directly use raw microseconds/milliseconds to create Timestamp
                if (fieldVector instanceof TimeStampMilliVector vector) {
                    // TimeStampMilliVector stores milliseconds (since Unix epoch, UTC time)
                    long millis = vector.get(index);
                    return new java.sql.Timestamp(millis);
                } else if (fieldVector instanceof TimeStampMicroVector vector) {
                    // TimeStampMicroVector stores microseconds (since Unix epoch, UTC time)
                    long micros = vector.get(index);
                    long millis = micros / 1000;
                    int microsPart = (int) (micros % 1000); // Microseconds part (0-999)
                    java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
                    // Timestamp constructor automatically sets nanoseconds for milliseconds part
                    // We need to preserve existing millisecond nanoseconds and add microsecond nanoseconds
                    // getNanos() returns nanoseconds part of seconds (0-999,999,999)
                    int existingNanos = timestamp.getNanos();
                    int additionalNanos = microsPart * 1000; // Convert microseconds to nanoseconds
                    timestamp.setNanos(existingNanos + additionalNanos);
                    return timestamp;
                } else if (fieldVector instanceof TimeStampNanoVector vector) {
                    // TimeStampNanoVector stores nanoseconds (since Unix epoch, UTC time)
                    long nanos = vector.get(index);
                    long millis = nanos / 1_000_000;
                    int nanosPart = (int) (nanos % 1_000_000);
                    java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
                    timestamp.setNanos((int) (timestamp.getNanos() / 1000000 * 1000000 + nanosPart));
                    return timestamp;
                } else if (fieldVector instanceof TimeStampSecVector vector) {
                    // TimeStampSecVector stores seconds (since Unix epoch, UTC time)
                    long seconds = vector.get(index);
                    long millis = seconds * 1000;
                    return new java.sql.Timestamp(millis);
                }
                log.debug("Unexpected Timestamp vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Interval -> {
                // Interval type is complex, need to handle based on specific subtype
                if (fieldVector instanceof IntervalYearVector vector) {
                    // YEAR_MONTH interval
                    return vector.getObject(index);
                } else if (fieldVector instanceof IntervalDayVector vector) {
                    // DAY_TIME interval
                    return vector.getObject(index);
                } else if (fieldVector instanceof IntervalMonthDayNanoVector vector) {
                    // MONTH_DAY_NANO interval
                    return vector.getObject(index);
                }
                log.debug("Unexpected Interval vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            case Null -> {
                return null;
            }
            case Bool -> {
                if (fieldVector instanceof BitVector vector) {
                    return vector.get(index) == 1;
                }
                log.debug("Unexpected Bool vector type: {}, using getObject() fallback", fieldVector.getClass().getSimpleName());
                return fieldVector.getObject(index);
            }
            default -> {
                log.debug("Unsupported Arrow type: {}, using getObject() fallback", arrowTypeID);
                return fieldVector.getObject(index);
            }
        }
    }

    /**
     * Build single record from VectorSchemaRoot.
     * 
     * @param root VectorSchemaRoot
     * @param rowIndex Row index
     * @return Record data Map
     */
    private Map<String, Object> buildRecord(VectorSchemaRoot root, int rowIndex) {
        Record record = new Record();
        int columnCount = root.getFieldVectors().size();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            String columnName = root.getVector(columnIndex).getField().getName().toLowerCase();
            record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
        }
        return record.getData();
    }

    @Override
    public void write(VectorSchemaRoot root) {
        final int batchSize = root.getRowCount();
        log.debug("Writing {} rows to table {}", batchSize, tableName);

        if (supportMultiInsert) {
            writeWithMultiInsert(root, batchSize);
        } else {
            writeWithSingleInsert(root, batchSize);
        }
    }

    /**
     * Batch insert mode: accumulate records and batch insert.
     */
    private void writeWithMultiInsert(VectorSchemaRoot root, int batchSize) {
        List<Map<String, Object>> multiRecords = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
            multiRecords.add(buildRecord(root, rowIndex));
            if (multiRecords.size() == BATCH_NUM) {
                this.insertMultiData(commandConfig.getResultSchema(), multiRecords);
                this.commitBatch();
                multiRecords.clear();
            }
        }
        if (!multiRecords.isEmpty()) {
            this.insertMultiData(commandConfig.getResultSchema(), multiRecords);
            this.commitBatch();
        }
    }

    /**
     * Single insert mode: insert row by row.
     */
    private void writeWithSingleInsert(VectorSchemaRoot root, int batchSize) {
        boolean autoCommit = true;
        try {
            autoCommit = connection.getAutoCommit();
        } catch (SQLException e) {
            log.warn("Failed to get autoCommit status, assuming true", e);
        }
        
        for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
            Map<String, Object> record = buildRecord(root, rowIndex);
            this.insertData(commandConfig.getResultSchema(), record);
            
            if (!autoCommit && (rowIndex + 1) % BATCH_NUM == 0) {
                this.commitBatch();
            }
        }
        if (!autoCommit && batchSize % BATCH_NUM != 0) {
            this.commitBatch();
        }
    }

    /**
     * Commit transaction (if manual transaction is enabled).
     * This is the core method that actually performs the commit.
     */
    private void commitTransaction() {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
                log.debug("Transaction committed");
            }
        } catch (SQLException e) {
            log.error("Failed to commit transaction", e);
            throw new RuntimeException("Failed to commit transaction", e);
        }
    }

    /**
     * Commit transaction per batch (internal use).
     * Automatically called during batch insert, commits every 500 rows.
     */
    private void commitBatch() {
        commitTransaction();
    }

    /**
     * Flush/commit transaction (public interface method).
     * Implements Writer interface, allows external explicit call to flush uncommitted data.
     */
    @Override
    public void flush() {
        commitTransaction();
    }

    public void close() {
        if (connection == null) {
            return;
        }
        
        try {
            // Rollback uncommitted transaction first if exists
            if (!connection.isClosed() && !connection.getAutoCommit()) {
                try {
                    connection.rollback();
                    log.debug("Rolled back uncommitted transaction on close");
                } catch (SQLException e) {
                    log.warn("Failed to rollback transaction on close", e);
                }
            }
            
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Database connection close error", e);
            throw new RuntimeException("Failed to close database connection", e);
        }
    }

    private void createTable(Schema schema){
        String createTableSql = this.buildCreateTableSql.apply(tableName, schema, partitionSpec);
        try (Statement stmt = connection.createStatement()){
            stmt.executeUpdate(createTableSql);
            log.info("Successfully created table: {}", tableName);
        } catch (SQLException e) {
            log.error("Failed to create table {}: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to create table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        // Only allows letters, numbers, underscores, and must start with a letter
        if (!tableName.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid table name format. Table name must start with a letter and contain only letters, numbers, and underscores");
        }
    }

    private void dropTable() throws SQLException {
        validateTableName(tableName);
        if (checkTableExists.apply(connection, tableName)) {
            String sql = "DROP TABLE " + tableName;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
                log.info("Successfully dropped table: {}", tableName);
            } catch (SQLException e) {
                log.error("Failed to drop table {}: {}", tableName, e.getMessage(), e);
                throw e;
            }
        }
    }

    private void deleteAllRowOfTable() throws SQLException {
        validateTableName(tableName);

        String sql = "DELETE FROM " + tableName;
        
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int deletedRows = preparedStatement.executeUpdate();
            log.info("Deleted {} rows from table {}", deletedRows, tableName);
        } catch (SQLException e) {
            log.error("Failed to delete data from table {}: {}", tableName, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Delete rows for specific partition (partition overwrite)
     * 
     * @param partitionSpec Partition specification map (e.g., {"dt": "20240101"})
     * @throws SQLException If deletion fails
     */
    private void deletePartitionData(Map<String, String> partitionSpec) throws SQLException {
        validateTableName(tableName);
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            log.warn("Partition spec is empty, skipping partition deletion");
            return;
        }
        
        // Build WHERE clause for partition deletion
        List<String> conditions = new ArrayList<>();
        for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // Validate partition key name (basic validation)
            if (!key.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                throw new IllegalArgumentException("Invalid partition key: " + key);
            }
            conditions.add(key + " = ?");
        }
        
        String whereClause = String.join(" AND ", conditions);
        String sql = "DELETE FROM " + tableName + " WHERE " + whereClause;
        
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int paramIndex = 1;
            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                preparedStatement.setString(paramIndex++, entry.getValue());
            }
            int deletedRows = preparedStatement.executeUpdate();
            log.info("Deleted {} rows from partition {} in table {}", deletedRows, partitionSpec, tableName);
        } catch (SQLException e) {
            log.error("Failed to delete partition data from table {}: partition={}, error: {}", 
                    tableName, partitionSpec, e.getMessage());
            throw e;
        }
    }

    public void insertData(Schema arrowSchema, Map<String, Object> data) {
        String sql = this.buildInsertSql.apply(tableName, arrowSchema, data, partitionSpec);
        try (PreparedStatement stmt = connection.prepareStatement(sql)){
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to insert data into table {}: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to insert data into table " + tableName + ": " + e.getMessage(), e);
        }
    }

    public void insertMultiData(Schema arrowSchema, List<Map<String, Object>> multiData){
        SqlWithParams sp = this.buildMultiInsertSql.apply(tableName, arrowSchema, multiData, partitionSpec);
        
        // Build parameter type mapping: determine each parameter's type based on Arrow Schema
        // Note: INTERVAL types skip parameter list (directly embedded in SQL), so parameter index and column index may not match
        List<ArrowType> paramTypes = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            ArrowType fieldType = field.getType();
            if (!(fieldType instanceof ArrowType.Interval)) {
                paramTypes.add(fieldType);
            }
        }
        
        try (PreparedStatement ps = connection.prepareStatement(sp.sql);){
            for (int i = 0; i < sp.params.size(); i++) {
                Object param = sp.params.get(i);
                // For multi-row insert, parameter list contains all rows' parameters, use modulo to get corresponding type
                ArrowType paramType = paramTypes.isEmpty() ? null : paramTypes.get(i % paramTypes.size());

                if (param instanceof Float) {
                    ps.setFloat(i + 1, ((Float) param).floatValue());
                } else if (param instanceof Double) {
                    ps.setDouble(i + 1, ((Double) param).doubleValue());
                } else if (param instanceof BigDecimal && paramType instanceof ArrowType.Decimal) {
                    // DECIMAL type: use setBigDecimal() to ensure precision and scale are correctly passed
                    ps.setBigDecimal(i + 1, (BigDecimal) param);
                } else if (paramType instanceof ArrowType.LargeUtf8) {
                    // CLOB type (LargeUtf8): must convert byte[] to String (UTF-8 decode)
                    String clobValue;
                    if (param instanceof byte[]) {
                        clobValue = new String((byte[]) param, StandardCharsets.UTF_8);
                        ps.setObject(i + 1, clobValue);
                        continue;
                    }
                }
                ps.setObject(i + 1, param);
            }
            int rowsAffected = ps.executeUpdate();
            log.debug("Inserted {} rows into table {}", multiData.size(), tableName);
        } catch (SQLException e) {
            log.error("Failed to insert {} rows into table {}: {}", multiData.size(), tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to insert data into table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private void preProcessing(String tableName){
        boolean tableExists = checkTableExists.apply(connection, tableName);
        
        if (tableExists) {
            // If partition spec exists, delete only partition data (partition overwrite)
            if (partitionSpec != null && !partitionSpec.isEmpty()) {
                try {
                    this.deletePartitionData(partitionSpec);
                    return; // Partition overwrite completed, no need to create table
                } catch (SQLException e) {
                    log.error("Failed to delete partition data from table {}: partition={}, error: {}", 
                            tableName, partitionSpec, e.getMessage(), e);
                    throw new RuntimeException("Failed to delete partition data from table " + tableName + 
                            ", partition=" + partitionSpec + ": " + e.getMessage(), e);
                }
            }
            
            // No partition spec, delete entire table (full table overwrite)
            try {
                this.dropTable();
            } catch (SQLException e) {
                log.error("Failed to drop table {}: {}", tableName, e.getMessage(), e);
            }
        }
        
        if(!checkTableExists.apply(connection, tableName)) {
            createTable(commandConfig.getResultSchema());
        } else if (partitionSpec == null || partitionSpec.isEmpty()) {
            throw new RuntimeException("Cannot create table " + tableName + ": table still exists after drop attempt");
        }
    }

    public static class SqlWithParams {

        public final String sql;
        public final List<Object> params;

        public SqlWithParams(String sql, List<Object> params) {
            this.sql = sql;
            this.params = params;
        }
        
        @Override
        public String toString() {
            return "SqlWithParams{" +
                    "sql='" + (sql.length() > 500 ? sql.substring(0, 500) + "..." : sql) + '\'' +
                    ", params=" + params.size() + " parameters" +
                    '}';
        }
    }

}
