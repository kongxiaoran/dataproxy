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

package org.secretflow.dataproxy.util;

import dm.jdbc.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition;

/**
 * Utility class for Dameng database operations.
 */
@Slf4j
public class DamengUtil {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    private static final Pattern PRECISION_PATTERN = Pattern.compile("\\((\\d+)\\)");

    /**
     * Initialize Dameng database connection.
     *
     * @param config Database connection configuration
     * @return JDBC connection object
     * @throws RuntimeException if connection fails
     */
    public static Connection initDameng(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        // Default Dameng database port
        int port = 5236;

        /*
         * Parse endpoint address (host:port)
         * Note: IPv6 format (e.g., [::1]:5236) is not supported, only IPv4 or hostname
         */
        if (endpoint.contains(":")) {
            // Limit split to avoid IPv6 address issues
            String[] parts = endpoint.split(":", 2);
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                try {
                    port = Integer.parseInt(parts[1]);
                    if (port < 1 || port > 65535) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                                "Invalid port number: " + port + ". Port must be between 1 and 65535.");
                    }
                } catch (NumberFormatException e) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                            "Invalid port format in endpoint: " + endpoint);
                }
            }
        } else {
            ip = endpoint;
        }

        // Validate IP address or hostname to prevent JDBC URL injection
        if (ip == null || !ip.matches("^[a-zA-Z0-9._-]+$") || ip.contains("..") ||
                ip.startsWith(".") || ip.endsWith(".")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid IP address or hostname: " + ip);
        }

        // Validate database name to prevent JDBC URL injection
        String database = config.database();
        if (database == null || !database.matches("^[a-zA-Z0-9_]+$")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid database name: " + database);
        }

        Connection conn;
        try {
            Class.forName("dm.jdbc.driver.DmDriver");
            String url = String.format("jdbc:dm://%s:%d/%s", ip, port, database);
            log.info("Connecting to Dameng database: {}", url.replaceAll("(password:)[^@]*", "$1****"));

            conn = DriverManager.getConnection(url, config.username(), config.password());
            log.info("Successfully connected to Dameng database");
        } catch (ClassNotFoundException e) {
            log.error("Dameng JDBC driver not found", e);
            throw new RuntimeException("Dameng JDBC driver not found. Please ensure DmJdbcDriver18 is in classpath.", e);
        } catch (SQLException e) {
            log.error("Failed to connect to Dameng database", e);
            throw new RuntimeException("Failed to connect to Dameng database: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error connecting to Dameng database", e);
            throw new RuntimeException(e);
        }
        return conn;
    }


    /**
     * Build SELECT query SQL statement based on table name, columns, and partition specification.
     *
     * @param tableName       Table name
     * @param columns         Column name list
     * @param partitionClause Partition specification (e.g., "dt=20240101")
     * @return SELECT SQL statement
     */
    public static String buildQuerySql(String tableName, List<String> columns, String partitionClause) {
        if (columns == null || columns.isEmpty()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "columns cannot be empty");
        }

        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        for (String field : columns) {
            if (!IDENTIFIER_PATTERN.matcher(field).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + field);
            }
        }

        String sql = "SELECT " + String.join(", ", columns) + " FROM " + tableName;

        if (partitionClause != null && !partitionClause.trim().isEmpty()) {
            final Map<String, String> partitionSpec = parsePartition(partitionClause);
            List<String> conditions = new ArrayList<>();

            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (!IDENTIFIER_PATTERN.matcher(key).matches()) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                }

                if (!value.matches("^[a-zA-Z0-9_.-]+$")) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + value);
                }

                conditions.add(key + "='" + escapeString(value) + "'");
            }
            String processedPartition = String.join(" AND ", conditions);

            sql += " WHERE " + processedPartition;
        }

        log.info("Built query SQL: {}", sql);
        return sql;
    }

    /**
     * Build CREATE TABLE SQL statement.
     *
     * @param tableName Table name
     * @param schema    Arrow Schema
     * @param partition Partition specification map (only for validating partition key names; partition columns must be included in column definitions in Dameng)
     * @return CREATE TABLE SQL statement
     */
    public static String buildCreateTableSql(String tableName, Schema schema, Map<String, String> partition) {
        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        List<Field> fields = schema.getFields();
        
        if (fields.isEmpty()) {
            throw DataproxyException.of(
                    DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Table must have at least one column. Empty schema is not allowed in Dameng database."
            );
        }

        for (Field field : fields) {
            String fieldName = field.getName();
            if (!IDENTIFIER_PATTERN.matcher(fieldName).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + fieldName);
            }
        }

        if (partition != null) {
            for (String partKey : partition.keySet()) {
                if (!IDENTIFIER_PATTERN.matcher(partKey).matches()) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + partKey);
                }
            }
        }

        // Note: In Dameng database, partition columns must be included in column definitions, unlike Hive where partition columns are defined separately in PARTITIONED BY clause
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        String columnDefinitions = fields.stream()
                .map(field -> "  " + field.getName() + " " + arrowTypeToJdbcType(field.getType()))
                .collect(Collectors.joining(",\n"));
        
        sb.append(columnDefinitions).append("\n)");

        log.info("Built CREATE TABLE SQL: {}", sb);
        return sb.toString();
    }


    /**
     * Convert Arrow type to Dameng JDBC type.
     *
     * @param arrowType Arrow type
     * @return JDBC type name
     */
    public static String arrowTypeToJdbcType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Utf8) {
            return "VARCHAR";
        } else if (arrowType instanceof ArrowType.LargeUtf8) {
            return "CLOB";
        }
        else if (arrowType instanceof ArrowType.Int intType) {
            int bitWidth = intType.getBitWidth();
            return switch (bitWidth) {
                case 8 -> "TINYINT";
                case 16 -> "SMALLINT";
                case 32 -> "INT";
                case 64 -> "BIGINT";
                default -> throw new IllegalArgumentException("Unsupported Int bitWidth: " + bitWidth);
            };
        }
        else if (arrowType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                default -> throw new IllegalArgumentException("Unsupported floating point type");
            };
        }
        else if (arrowType instanceof ArrowType.Bool) {
            // Dameng uses BIT type for boolean: 1=true, 0=false, NULL=null
            return "BIT";
        }
        else if (arrowType instanceof ArrowType.Date dateType) {
            return switch (dateType.getUnit()) {
                case DAY -> "DATE";
                // Date(MILLISECOND) maps to DATETIME(3), distinct from Timestamp
                case MILLISECOND -> "DATETIME(3)";
            };
        } else if (arrowType instanceof ArrowType.Time timeType) {
            /*
             * Arrow Time type bitWidth must be 32 or 64 (standard), not precision
             * For Dameng database, determine precision based on TimeUnit instead of bitWidth
             */
            int precision = switch (timeType.getUnit()) {
                // TIME(0) - seconds precision
                case SECOND -> 0;
                // TIME(3) - milliseconds precision
                case MILLISECOND -> 3;
                // TIME(6) - microseconds precision
                case MICROSECOND -> 6;
                case NANOSECOND -> {
                    log.warn("Dameng database does not support nanosecond precision for TIME type. Using microseconds (6) instead.");
                    // Fallback to microseconds
                    yield 6;
                }
            };
            return "TIME(" + precision + ")";
        } else if (arrowType instanceof ArrowType.Timestamp timestampType) {
            /*
             * Only precision is specified when creating table; timezone info is not specified at table creation
             * It needs to be concatenated with TIMESTAMP when inserting data, format: 2002.12.12 09:10:21 -5:00
             */
            String damengType = "TIMESTAMP";
            if (StringUtil.isNotEmpty(timestampType.getTimezone())) {
                damengType = "TIMESTAMP WITH TIME ZONE";
            }
            return damengType + switch (timestampType.getUnit()) {
                case SECOND -> "(0)";
                case MILLISECOND -> "(3)";
                case MICROSECOND -> "(6)";
                case NANOSECOND -> throw new IllegalArgumentException(
                        "Dameng currently does not support nanosecond level accuracy");
            };
        }
        else if (arrowType instanceof ArrowType.Decimal dec) {
            int precision = dec.getPrecision();
            int scale = dec.getScale();

            // Validate Dameng database precision limits: precision range 1-38, scale range 0-precision
            if (precision < 1 || precision > 38) {
                throw new IllegalArgumentException(
                        String.format("DECIMAL precision %d out of range [1, 38] for Dameng database", precision));
            }
            if (scale < 0 || scale > precision) {
                throw new IllegalArgumentException(
                        String.format("DECIMAL scale %d out of range [0, %d]", scale, precision));
            }
            return "DECIMAL(" + precision + ", " + scale + ")";
        }
        else if (arrowType instanceof ArrowType.Binary) {
            // Dameng database VARBINARY default length is 8188 bytes
            return "VARBINARY";
        } else if (arrowType instanceof ArrowType.FixedSizeBinary fixedBinary) {
            int byteWidth = fixedBinary.getByteWidth();
            if (byteWidth <= 8188) {
                // Dameng database BINARY maximum length is 8188 bytes
                return "BINARY(" + byteWidth + ")";
            } else {
                log.warn("FixedSizeBinary byteWidth {} exceeds BINARY/VARBINARY limit (8188), using BLOB", byteWidth);
                return "BLOB";
            }
        } else if (arrowType instanceof ArrowType.LargeBinary) {
            // Large binary object (maximum length 100G-1 bytes)
            return "BLOB";
        }
        else if (arrowType instanceof ArrowType.Interval intervalType) {
            return switch (intervalType.getUnit()) {
                case YEAR_MONTH -> "INTERVAL YEAR TO MONTH";
                case DAY_TIME -> "INTERVAL DAY TO SECOND";
                default -> throw new IllegalArgumentException("Unsupported Interval unit: " + intervalType.getUnit());
            };
        }
        else if (arrowType instanceof ArrowType.Null) {
            // NULL type is usually not used alone in actual table creation, return VARCHAR as placeholder
            return "VARCHAR(1)";
        }
        else {
            throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType.getClass().getName());
        }
    }

    /**
     * Extract precision information from JDBC type name.
     * Example: TIMESTAMP(3) -> 3, DATETIME(3) -> 3
     * 
     * @param jdbcType JDBC type name (may contain precision, e.g., "TIMESTAMP(3)")
     * @return Precision value, or null if no precision information
     */
    private static Integer parsePrecisionFromTypeName(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            return null;
        }
        java.util.regex.Matcher matcher = PRECISION_PATTERN.matcher(jdbcType);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                log.warn("Failed to parse precision from JDBC type: {}", jdbcType);
                return null;
            }
        }
        return null;
    }

    /**
     * Extract base type name from JDBC type name (remove precision information).
     * Example: TIMESTAMP(3) -> TIMESTAMP, DATETIME(3) -> DATETIME
     * 
     * @param jdbcType JDBC type name
     * @return Base type name
     */
    private static String extractBaseType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            return jdbcType;
        }
        return jdbcType.replaceAll("\\(\\d+\\)", "").trim().toUpperCase();
    }

    /**
     * Convert Dameng JDBC type to Arrow type.
     * Note: This method attempts to parse precision information from TYPE_NAME (e.g., TIMESTAMP(3))
     *
     * @param jdbcType JDBC type name (may contain precision, e.g., "TIMESTAMP(3)" or "DATETIME(3)")
     * @return Arrow type
     */
    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            throw new IllegalArgumentException("JDBC type is null or empty");
        }
        
        String baseType = extractBaseType(jdbcType);
        Integer precision = parsePrecisionFromTypeName(jdbcType);
        
        return switch (baseType) {
            case "DECIMAL", "NUMERIC", "DEC", "NUMBER" -> {
                // Use default precision if precision info is not provided (backward compatibility)
                yield new ArrowType.Decimal(38, 10, 128);
            }

            case "TIMESTAMP" -> {
                org.apache.arrow.vector.types.TimeUnit timeUnit;
                if (precision != null && precision <= 3) {
                    // TIMESTAMP(0-3): millisecond precision
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
                } else if (precision != null && precision <= 6) {
                    // TIMESTAMP(4-6): microsecond precision
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
                } else {
                    // Default to microsecond precision if precision is unknown (for compatibility)
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
                }
                yield new ArrowType.Timestamp(timeUnit, null);
            }
            
            /*
             * DATETIME(3) -> Date(MILLISECOND)
             * DATETIME(6) -> Timestamp(MICROSECOND) (Date doesn't support microseconds, use Timestamp)
             */
            case "DATETIME" -> {
                if (precision != null && precision == 3) {
                    // DATETIME(3): millisecond precision -> Date(MILLISECOND)
                    yield new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.MILLISECOND);
                } else if (precision != null && precision == 6) {
                    // DATETIME(6): microsecond precision -> Timestamp(MICROSECOND) (Date doesn't support microseconds)
                    log.warn("DATETIME(6) precision not supported by Date type, mapping to Timestamp(MICROSECOND)");
                    yield new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
                } else {
                    // Default to millisecond precision if precision is unknown
                    yield new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.MILLISECOND);
                }
            }

            /*
             * Arrow standard: bitWidth must be 32 or 64, not precision
             * Use Time32 (32-bit) for millisecond precision, Time64 (64-bit) for microsecond precision
             * TIME(3) -> Time(MILLISECOND, 32) -> TimeMilliVector
             * TIME(6) -> Time(MICROSECOND, 64) -> TimeMicroVector
             */
            case "TIME" -> {
                org.apache.arrow.vector.types.TimeUnit timeUnit;
                int bitWidth;
                if (precision != null && precision <= 3) {
                    // TIME(0-3): millisecond precision -> Time32
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
                    bitWidth = 32;
                } else if (precision != null && precision <= 6) {
                    // TIME(4-6): microsecond precision -> Time64
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
                    bitWidth = 64;
                } else {
                    // Default to millisecond precision if precision is unknown
                    timeUnit = org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
                    bitWidth = 32;
                }
                yield new ArrowType.Time(timeUnit, bitWidth);
            }

            case "TIMESTAMP_WITH_TIMEZONE", "TIMESTAMP WITH TIME ZONE", 
                 "TIMESTAMP WITH LOCAL TIME ZONE", "DATETIME_TZ", "DATETIME WITH TIME ZONE" -> {
                yield new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
            }

            case "BINARY" -> {
                yield new ArrowType.Binary();
            }

            case "CHAR", "CHARACTER", "VARCHAR", "VARCHAR2" -> ArrowType.Utf8.INSTANCE;
            // Large string types - TEXT/LONG/LONGVARCHAR/CLOB all map to LargeUtf8
            case "TEXT", "LONG", "LONGVARCHAR", "CLOB" -> ArrowType.LargeUtf8.INSTANCE;

            // BYTE is an alias for TINYINT
            case "TINYINT", "BYTE" -> new ArrowType.Int(8, true);
            case "SMALLINT" -> new ArrowType.Int(16, true);
            case "INT", "INTEGER", "PLS_INTEGER" -> new ArrowType.Int(32, true);
            case "BIGINT" -> new ArrowType.Int(64, true);

            case "FLOAT", "REAL" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "DOUBLE", "DOUBLE PRECISION" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

            // Dameng uses BIT type for boolean: 1=true, 0=false, NULL=null
            case "BOOLEAN", "BIT" -> ArrowType.Bool.INSTANCE;

            case "DATE" -> new ArrowType.Date(DateUnit.DAY);

            // RAW is an alias for VARBINARY
            case "VARBINARY", "RAW" -> ArrowType.Binary.INSTANCE;

            // Large binary types - IMAGE/LONGVARBINARY/BLOB all map to LargeBinary
            case "IMAGE", "LONGVARBINARY", "BLOB" -> ArrowType.LargeBinary.INSTANCE;

            // Year-month interval types: all year-month related interval types map to YEAR_MONTH
            case "INTERVAL_YM", "INTERVAL YEAR TO MONTH", "INTERVAL YEAR", "INTERVAL MONTH" ->
                    new ArrowType.Interval(IntervalUnit.YEAR_MONTH);

            // Day-time interval types: all day-time related interval types map to DAY_TIME
            case "INTERVAL_DT", "INTERVAL DAY TO TIME", "INTERVAL DAY TO SECOND",
                 "INTERVAL DAY", "INTERVAL DAY TO HOUR", "INTERVAL DAY TO MINUTE",
                 "INTERVAL HOUR", "INTERVAL HOUR TO MINUTE", "INTERVAL HOUR TO SECOND",
                 "INTERVAL MINUTE", "INTERVAL MINUTE TO SECOND", "INTERVAL SECOND" ->
                    new ArrowType.Interval(IntervalUnit.DAY_TIME);

            case "NULL" -> ArrowType.Null.INSTANCE;

            default -> {
                log.warn("Unsupported JDBC type: {}, using Utf8 as fallback", jdbcType);
                yield ArrowType.Utf8.INSTANCE;
            }
        };
    }



    public DamengUtil() {
    }

    /**
     * Build multi-row INSERT SQL statement (parameterized).
     *
     * @param tableName Table name
     * @param schema Arrow Schema
     * @param dataList Data row list
     * @param partition Partition specification map
     * @return SqlWithParams object (contains SQL and parameter list)
     */
    public static DatabaseRecordWriter.SqlWithParams buildMultiRowInsertSql(String tableName,
                                                                            Schema schema,
                                                                            List<Map<String, Object>> dataList,
                                                                            Map<String, String> partition) {
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("No data to insert");
        }

        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        for (Field f : schema.getFields()) {
            if (!IDENTIFIER_PATTERN.matcher(f.getName()).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + f.getName());
            }
        }

        for (String k : partition.keySet()) {
            if (!IDENTIFIER_PATTERN.matcher(k).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + k);
            }
        }

        List<Field> fields = schema.getFields();
        List<String> allColumns = fields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());
        Map<String, Field> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, f -> f));

        // Build SQL statement (INTERVAL types use literal embedding)
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append(" (").append(String.join(", ", allColumns)).append(")");
        sb.append(" VALUES ");

        // Build VALUES clause for each row (INTERVAL types directly embed literals)
        List<Object> params = new ArrayList<>();
        List<String> valueClauses = new ArrayList<>();
        Set<String> partitionKeys = partition.keySet();
        // Define datetime format with timezone offset supported by Dameng
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");

        for (Map<String, Object> row : dataList) {
            List<String> rowValues = new ArrayList<>();
            for (String colName : allColumns) {
                Object value;
                if (partitionKeys.contains(colName)) {
                    value = partition.get(colName);
                } else {
                    // Use lowercase column name to get value, because DatabaseRecordWriter.write() uses lowercase keys
                    value = row.get(colName.toLowerCase());
                }

                Field field = fieldMap.get(colName);
                ArrowType fieldType = field.getType();

                if (fieldType instanceof ArrowType.Timestamp tsType) {
                    // Handle timestamp with timezone
                    if (tsType.getTimezone() != null && !tsType.getTimezone().isEmpty() && value instanceof Long) {
                        Instant instant = Instant.ofEpochMilli((Long) value);
                        ZoneId zoneId = ZoneId.of(tsType.getTimezone());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
                        // Convert to string format supported by Dameng
                        value = dtf.format(zonedDateTime);
                    }
                } else if (fieldType instanceof ArrowType.Bool) {
                    if (value instanceof Boolean) {
                        value = ((Boolean) value) ? 1 : 0;
                    }
                } else if (fieldType instanceof ArrowType.Interval intervalType) {
                    // INTERVAL type: use literal directly in SQL to avoid JDBC driver parsing errors
                    String intervalLiteral;
                    switch (intervalType.getUnit()){
                        case YEAR_MONTH:
                            int totalMonths = 0;
                            boolean converted = false;
                            if (value instanceof Integer) {
                                totalMonths = (Integer) value;
                                converted = true;
                            } else if (value instanceof Long) {
                                totalMonths = ((Long) value).intValue();
                                converted = true;
                            } else if (value instanceof String) {
                                try {
                                    totalMonths = Integer.parseInt((String) value);
                                    converted = true;
                                } catch (NumberFormatException e) {
                                    log.warn("Failed to parse YEAR_MONTH string value: {}", value);
                                }
                            }
                            
                            if (converted) {
                                int years = totalMonths / 12;
                                int months = totalMonths % 12;
                                // Handle negative numbers: Java's % operator may return negative for negative numbers
                                if (totalMonths < 0 && months < 0) {
                                    months = 12 + months;
                                    years = years - 1;
                                }
                                if (totalMonths < 0) {
                                    intervalLiteral = String.format("INTERVAL '-%d-%d' YEAR TO MONTH", Math.abs(years), Math.abs(months));
                                } else {
                                    intervalLiteral = String.format("INTERVAL '%d-%d' YEAR TO MONTH", years, months);
                                }
                            } else if (value instanceof Period period) {
                                // Period may not be normalized, need to normalize manually
                                int periodYears = period.getYears();
                                int periodMonths = period.getMonths();
                                int periodTotalMonths = periodYears * 12 + periodMonths;
                                int years = periodTotalMonths / 12;
                                int months = periodTotalMonths % 12;
                                // Handle negative numbers
                                if (periodTotalMonths < 0 && months < 0) {
                                    months = 12 + months;
                                    years = years - 1;
                                }
                                if (periodTotalMonths < 0) {
                                    intervalLiteral = String.format("INTERVAL '-%d-%d' YEAR TO MONTH", Math.abs(years), Math.abs(months));
                                } else {
                                    intervalLiteral = String.format("INTERVAL '%d-%d' YEAR TO MONTH", years, months);
                                }
                            } else {
                                log.warn("Unexpected type for YEAR_MONTH interval, expected Integer/Long/String or Period but got {}. Setting to null.",
                                        value != null ? value.getClass().getName() : "null");
                                intervalLiteral = "NULL";
                            }
                            break;
                        case DAY_TIME:
                            Duration duration = null;
                            if (value instanceof PeriodDuration pd) {
                                duration = pd.getDuration();
                            } else if (value instanceof Duration) {
                                duration = (Duration) value;
                            } else {
                                log.warn("Unexpected type for DAY_TIME interval, expected PeriodDuration or Duration but got {}. Setting to null.",
                                        value != null ? value.getClass().getName() : "null");
                                intervalLiteral = "NULL";
                                break;
                            }
                            
                            // Extract time components (unified handling of PeriodDuration and Duration)
                            long totalSeconds = duration.getSeconds();
                            int nano = duration.getNano();
                            
                            long days = totalSeconds / (24 * 60 * 60);
                            long hours = (totalSeconds % (24 * 60 * 60)) / (60 * 60);
                            long minutes = (totalSeconds % (60 * 60)) / 60;
                            long seconds = totalSeconds % 60;
                            long millis = nano / 1_000_000;

                            intervalLiteral = String.format("INTERVAL '%d %02d:%02d:%02d.%03d' DAY TO SECOND(3)",
                                    days, hours, minutes, seconds, millis);
                            break;
                        case MONTH_DAY_NANO:
                            // Dameng database does not support MONTH_DAY_NANO interval type
                            log.warn("Dameng database does not support MONTH_DAY_NANO interval type. Setting value to null.");
                            intervalLiteral = "NULL";
                            break;
                        default:
                            log.warn("Unsupported interval unit: {}. Setting value to null.", intervalType.getUnit());
                            intervalLiteral = "NULL";
                            break;
                    }
                    // Use literal directly, do not add to parameter list
                    rowValues.add(intervalLiteral);
                    // Skip adding to params
                    continue;
                }
                
                // General type conversion: handle Java types not supported by Dameng JDBC driver
                if (value != null && (value instanceof Period || value instanceof Duration || value instanceof PeriodDuration)) {
                    log.warn("Unhandled temporal type: {}, setting to null", value.getClass().getName());
                    value = null;
                }
                
                // Non-INTERVAL types: use parameter placeholder
                rowValues.add("?");
                params.add(value);
            }
            valueClauses.add("(" + String.join(", ", rowValues) + ")");
        }
        
        sb.append(String.join(", ", valueClauses));

        return new DatabaseRecordWriter.SqlWithParams(sb.toString(), params);
    }

    /**
     * Check if table exists in the database.
     *
     * @param connection Database connection
     * @param tableName  Table name to check
     * @return true if table exists, false otherwise
     */
    public static boolean checkTableExists(Connection connection, String tableName) {
        // Validate table name (prevent SQL injection)
        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        /*
         * Dameng SQL to check table existence - use user table view
         * Note: Dameng database USER_TABLES.TABLE_NAME is uppercase, need to use UPPER() for case-insensitive comparison
         * Table name has been validated by identifier pattern, safe to concatenate SQL
         */
        String upperTableName = tableName.toUpperCase();
        String sql = "SELECT COUNT(*) FROM USER_TABLES WHERE UPPER(TABLE_NAME) = '" + upperTableName + "'";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            log.error("Error checking table existence: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to check table existence: " + e.getMessage(), e);
        }
    }


    /**
     * Escape SQL string to prevent SQL injection.
     * 
     * @param str String to escape
     * @return Escaped string, or empty string if input is null
     */
    private static String escapeString(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''");
    }

}
