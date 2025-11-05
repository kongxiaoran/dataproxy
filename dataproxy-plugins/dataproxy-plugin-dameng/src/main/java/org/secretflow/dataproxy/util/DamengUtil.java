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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.parsePartition;

/**
 * @Description: DamengUtil
 * @Author: kxr
 * @Date: 2025/11/5
 */
@Slf4j
public class DamengUtil {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");

    /**
     * 初始化达梦数据库连接。
     *
     * @param config 数据库连接配置
     * @return JDBC连接对象
     * @throws RuntimeException 如果连接失败
     */
    public static Connection initDameng(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        int port = 5236; // 达梦数据库默认端口

        // 解析端点地址（主机:端口）
        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }

        // 验证IP地址或主机名，防止JDBC URL注入
        if (ip == null || !ip.matches("^[a-zA-Z0-9._-]+$") || ip.contains("..") ||
                ip.startsWith(".") || ip.endsWith(".")) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Invalid IP address or hostname: " + ip);
        }

        // 验证数据库名，防止JDBC URL注入
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
     * 根据表名、列名和分区规范构建SELECT查询SQL语句。
     *
     * @param tableName       表名
     * @param columns         列名列表
     * @param partitionClause 分区规范（例如："dt=20240101"）
     * @return SELECT SQL语句
     */
    public static String buildQuerySql(String tableName, List<String> columns, String partitionClause) {

        // 验证列是否为空
        if (columns == null || columns.isEmpty()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "columns cannot be empty");
        }

        // 验证表名
        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        // 验证字段名
        for (String field : columns) {
            if (!IDENTIFIER_PATTERN.matcher(field).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + field);
            }
        }

        // 构建查询SQL
        String sql = "SELECT " + String.join(", ", columns) + " FROM " + tableName;

        // 处理分区子句
        if (partitionClause != null && !partitionClause.trim().isEmpty()) {

            final Map<String, String> partitionSpec = parsePartition(partitionClause);
            List<String> conditions = new ArrayList<>();

            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                // 验证分区键名
                if (!IDENTIFIER_PATTERN.matcher(key).matches()) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                }

                // 验证分区值
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
     * 构建CREATE TABLE SQL语句。
     *
     * @param tableName 表名
     * @param schema    Arrow Schema
     * @param partition 分区规范映射
     * @return CREATE TABLE SQL语句
     */
    public static String buildCreateTableSql(String tableName, Schema schema, Map<String, String> partition) {
        // 验证表名
        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partition.keySet();

        // 验证所有字段名
        for (Field field : fields) {
            String fieldName = field.getName();
            if (!IDENTIFIER_PATTERN.matcher(fieldName).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + fieldName);
            }
        }

        // 验证分区键名
        for (String partKey : partitionKeys) {
            if (!IDENTIFIER_PATTERN.matcher(partKey).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + partKey);
            }
        }

        // 表字段（包括分区字段）
        // 注意：在达梦数据库中，分区列必须包含在列定义中，与Hive不同，Hive的分区列在PARTITIONED BY子句中单独定义
        boolean first = true;
        for (Field field : fields) {
            String fieldName = field.getName();
            // 不要跳过分区字段 - 达梦数据库中分区字段必须在列定义中

            if (!first) {
                sb.append(",\n");
            }
            sb.append("  ").append(fieldName)
                    .append(" ")
                    .append(arrowTypeToJdbcType(field.getType()));
            first = false;
        }

        // 验证：表必须至少有一个列
        if (fields.isEmpty()) {
            throw DataproxyException.of(
                    DataproxyErrorCode.PARAMS_UNRELIABLE,
                    "Table must have at least one column. Empty schema is not allowed in Dameng database."
            );
        }

        sb.append("\n)");

        log.info("Built CREATE TABLE SQL: {}", sb);
        return sb.toString();
    }


    /**
     * 将Arrow类型转换为达梦JDBC类型。
     *
     * @param arrowType Arrow类型
     * @return JDBC类型名
     */
    public static String arrowTypeToJdbcType(ArrowType arrowType) {
        // 字符串类型
        if (arrowType instanceof ArrowType.Utf8) {
            return "VARCHAR";
        } else if (arrowType instanceof ArrowType.LargeUtf8) {
            return "CLOB"; // 大字符串类型映射到CLOB
        }
        // 整数类型
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
        // 浮点数类型
        else if (arrowType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                default -> throw new IllegalArgumentException("Unsupported floating point type");
            };
        }
        // 布尔类型
        else if (arrowType instanceof ArrowType.Bool) {
            return "BIT"; // 达梦数据库使用BIT类型表示布尔值：1=真，0=假，NULL=空
        }
        // 日期时间类型
        else if (arrowType instanceof ArrowType.Date dateType) {
            return switch (dateType.getUnit()) {
                case DAY -> "DATE";
                case MILLISECOND -> "TIMESTAMP(3)";
            };
        } else if (arrowType instanceof ArrowType.Time timeType) {
            if (timeType.getBitWidth() < 0 || timeType.getBitWidth() > 6) {
                throw new IllegalArgumentException(
                        String.format("Time bitWidth %d out of range [0, 6] for Dameng database", timeType.getBitWidth()));
            }
            return "TIME(" + timeType.getBitWidth() + ")";
        } else if (arrowType instanceof ArrowType.Timestamp timestampType) {
            // 建表时只指定精度，具体时区信息不是在建表时指定。而是需要在插入数据的时候一起拼接在 TIMESTAMP后，格式类似：2002.12.12 09:10:21 -5:00
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
        // 十进制类型
        else if (arrowType instanceof ArrowType.Decimal dec) {
            int precision = dec.getPrecision();
            int scale = dec.getScale();

            // 验证达梦数据库精度限制：精度范围 1-38，标度范围 0-precision
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
        // 二进制类型
        else if (arrowType instanceof ArrowType.Binary) {
            // 达梦数据库 VARBINARY 默认长度为 8188 字节
            return "VARBINARY";
        } else if (arrowType instanceof ArrowType.FixedSizeBinary fixedBinary) {
            // 定长二进制，需要长度信息
            int byteWidth = fixedBinary.getByteWidth();
            if (byteWidth <= 8188) {
                // 达梦数据库 BINARY 最大长度为 8188 字节
                return "BINARY(" + byteWidth + ")";
            } else {
                // 超过 8188 字节，使用 BLOB 避免数据截断
                log.warn("FixedSizeBinary byteWidth {} exceeds BINARY/VARBINARY limit (8188), using BLOB", byteWidth);
                return "BLOB";
            }
        } else if (arrowType instanceof ArrowType.LargeBinary) {
            return "BLOB"; // 大二进制对象（长度最大为 100G-1 字节）
        }
        // 间隔类型
        else if (arrowType instanceof ArrowType.Interval intervalType) {
            return switch (intervalType.getUnit()) {
                case YEAR_MONTH -> "INTERVAL YEAR TO MONTH";
                case DAY_TIME -> "INTERVAL DAY TO SECOND";
                default -> throw new IllegalArgumentException("Unsupported Interval unit: " + intervalType.getUnit());
            };
        }
        // 空值类型
        else if (arrowType instanceof ArrowType.Null) {
            // NULL类型在实际建表中通常不会单独使用，这里返回VARCHAR作为占位符
            return "VARCHAR(1)";
        }
        // 不支持的类型
        else {
            throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType.getClass().getName());
        }
    }

    /**
     * 将达梦JDBC类型转换为Arrow类型
     *
     * @param jdbcType JDBC类型名
     * @return Arrow类型
     */
    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            throw new IllegalArgumentException("JDBC type is null or empty");
        }
        String type = jdbcType.trim().toUpperCase();
        return switch (type) {
            // 处理DECIMAL类型及其别名
            // 达梦 JDBC 驱动返回的 TYPE_NAME 可能是 "DECIMAL"、"NUMERIC"、"DEC"、"NUMBER"
            // 精度信息在 COLUMN_SIZE（精度）和 DECIMAL_DIGITS（标度）中
            case "DECIMAL", "NUMERIC", "DEC", "NUMBER" -> {
                // 如果没有传递精度信息，使用默认精度（向后兼容）
                yield new ArrowType.Decimal(38, 10, 128); // 默认精度
            }

            // 处理TIMESTAMP类型 - 根据decimalDigits确定时间精度
            case "TIMESTAMP", "DATETIME" -> {
                yield new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
            }

            // 处理TIME类型 - 根据decimalDigits确定时间精度
            case "TIME" -> {
                yield new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32);
            }

            // 处理带时区的时间戳类型 - 根据decimalDigits确定精度
            case "TIMESTAMP_WITH_TIMEZONE", "TIMESTAMP WITH TIME ZONE", 
                 "TIMESTAMP WITH LOCAL TIME ZONE", "DATETIME_TZ", "DATETIME WITH TIME ZONE" -> {
                yield new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
            }

            // 处理BINARY类型
            case "BINARY" -> {
                yield new ArrowType.Binary();
            }

            // 字符串类型
            case "CHAR", "CHARACTER", "VARCHAR", "VARCHAR2" -> ArrowType.Utf8.INSTANCE;
            // 大字符串类型 - TEXT/LONG/LONGVARCHAR/CLOB 都映射到 LargeUtf8
            case "TEXT", "LONG", "LONGVARCHAR", "CLOB" -> ArrowType.LargeUtf8.INSTANCE;

            // 整数类型
            case "TINYINT", "BYTE" -> new ArrowType.Int(8, true); // BYTE 是 TINYINT 的别名
            case "SMALLINT" -> new ArrowType.Int(16, true);
            case "INT", "INTEGER", "PLS_INTEGER" -> new ArrowType.Int(32, true);
            case "BIGINT" -> new ArrowType.Int(64, true);

            // 浮点数类型
            case "FLOAT", "REAL" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "DOUBLE", "DOUBLE PRECISION" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

            // 布尔类型
            case "BOOLEAN", "BIT" -> ArrowType.Bool.INSTANCE; // 达梦数据库使用BIT类型表示布尔值：1=真，0=假，NULL=空

            // 日期类型
            case "DATE" -> new ArrowType.Date(DateUnit.DAY);

            // 变长二进制类型
            case "VARBINARY", "RAW" -> ArrowType.Binary.INSTANCE; // RAW 是 VARBINARY 的别名

            // 大二进制类型 - IMAGE/LONGVARBINARY/BLOB 都映射到 LargeBinary
            case "IMAGE", "LONGVARBINARY", "BLOB" -> ArrowType.LargeBinary.INSTANCE;

            // 达梦时间间隔类型
            // 年-月间隔类：所有年月相关的间隔类型都映射到 YEAR_MONTH
            case "INTERVAL_YM", "INTERVAL YEAR TO MONTH", "INTERVAL YEAR", "INTERVAL MONTH" ->
                    new ArrowType.Interval(IntervalUnit.YEAR_MONTH);

            // 日-时间隔类：所有日时相关的间隔类型都映射到 DAY_TIME
            case "INTERVAL_DT", "INTERVAL DAY TO TIME", "INTERVAL DAY TO SECOND",
                 "INTERVAL DAY", "INTERVAL DAY TO HOUR", "INTERVAL DAY TO MINUTE",
                 "INTERVAL HOUR", "INTERVAL HOUR TO MINUTE", "INTERVAL HOUR TO SECOND",
                 "INTERVAL MINUTE", "INTERVAL MINUTE TO SECOND", "INTERVAL SECOND" ->
                    new ArrowType.Interval(IntervalUnit.DAY_TIME);

            // 空值类型
            case "NULL" -> ArrowType.Null.INSTANCE;

            default -> {
                log.warn("Unsupported JDBC type: {}, using Utf8 as fallback", jdbcType);
                yield ArrowType.Utf8.INSTANCE;
            }
        };
    }

    /**
     * 根据小数位数（decimalDigits）确定Arrow时间单位。
     * JDBC规范：decimalDigits表示秒的小数部分精度
     *
     * @param decimalDigits 小数位数（0=秒，3=毫秒，6=微秒，9=纳秒）
     * @return Arrow时间单位
     */
    private static org.apache.arrow.vector.types.TimeUnit determineTimeUnit(Integer decimalDigits) {
        if (decimalDigits == null || decimalDigits <= 0) {
            return org.apache.arrow.vector.types.TimeUnit.SECOND;
        } else if (decimalDigits <= 3) {
            return org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
        } else if (decimalDigits <= 6) {
            return org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
        } else {
            // 达梦数据库不支持纳秒级精度，最大支持微秒（6位小数）
            log.warn("Dameng database does not support nanosecond precision. Using MICROSECOND instead for decimalDigits: {}", decimalDigits);
            return org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
        }
    }


    public DamengUtil() {
    }

    /**
     * 构建多行INSERT SQL语句（参数化）。
     *
     * @param tableName 表名
     * @param schema Arrow Schema
     * @param dataList 数据行列表
     * @param partition 分区规范映射
     * @return SqlWithParams对象（包含SQL和参数列表）
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

        // 验证字段名
        for (Field f : schema.getFields()) {
            if (!IDENTIFIER_PATTERN.matcher(f.getName()).matches()) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid field name:" + f.getName());
            }
        }

        // 验证分区键名
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

        // 2. 构建 SQL 语句
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append(" (").append(String.join(", ", allColumns)).append(")");
        sb.append(" VALUES ");
        String singleRowPlaceholder = "(" + String.join(", ", Collections.nCopies(allColumns.size(), "?")) + ")";
        sb.append(String.join(", ", Collections.nCopies(dataList.size(), singleRowPlaceholder)));

        // 3. 构建参数列表
        List<Object> params = new ArrayList<>();
        Set<String> partitionKeys = partition.keySet();
        // 定义达梦支持的带时区偏移的日期时间格式
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");

        for (Map<String, Object> row : dataList) {
            for (String colName : allColumns) {
                Object value;
                if (partitionKeys.contains(colName)) {
                    value = partition.get(colName);
                } else {
                    value = row.get(colName);
                }

                Field field = fieldMap.get(colName);
                ArrowType fieldType = field.getType();

                // 4. 恢复并增强类型转换
                if (fieldType instanceof ArrowType.Timestamp tsType) {
                    // 新增：处理带时区的时间戳
                    if (tsType.getTimezone() != null && !tsType.getTimezone().isEmpty() && value instanceof Long) {
                        Instant instant = Instant.ofEpochMilli((Long) value);
                        ZoneId zoneId = ZoneId.of(tsType.getTimezone());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
                        value = dtf.format(zonedDateTime); // 转换为达梦支持的字符串格式
                    }
                } else if (fieldType instanceof ArrowType.Bool) {
                    if (value instanceof Boolean) {
                        value = ((Boolean) value) ? 1 : 0;
                    }
                } else if (fieldType instanceof ArrowType.Interval) {
                    ArrowType.Interval intervalType = (ArrowType.Interval) fieldType;
                    switch (intervalType.getUnit()){
                        case YEAR_MONTH:
                            if (value instanceof Integer) {
                                int totalMonths = (Integer) value;
                                int years = totalMonths / 12;
                                int months = totalMonths % 12;
                                value = String.format("INTERVAL '%d-%d' YEAR TO MONTH", years, months);
                            } else {
                                log.warn("Unexpected type for YEAR_MONTH interval, expected Integer but got {}. Keep original value.",
                                        value.getClass().getName());
                            }
                            break;
                        case DAY_TIME:
                            if (value instanceof PeriodDuration) {
                                PeriodDuration pd = (PeriodDuration) value;
                                long seconds = pd.getDuration().getSeconds();
                                int nano = pd.getDuration().getNano();

                                // 使用秒信息来计算 天、时、分、秒，使用纳秒来记录不足1秒的间隔
                                long days = TimeUnit.NANOSECONDS.toDays(seconds);
                                long hours = TimeUnit.NANOSECONDS.toHours(seconds);
                                long minutes = TimeUnit.NANOSECONDS.toMinutes(seconds) % 60;
                                long millis = TimeUnit.NANOSECONDS.toMillis(nano) % 1000;

                                value = String.format("INTERVAL '%d %02d:%02d:%02d.%03d' DAY TO SECOND(3)",
                                        days, hours, minutes, seconds, millis);
                            } else {
                                log.warn("Unexpected type for DAY_TIME interval, expected PeriodDuration but got {}. Keep original value.",
                                        value.getClass().getName());
                            }
                            break;
                        case MONTH_DAY_NANO:
                            // 达梦数据库不支持 MONTH_DAY_NANO 间隔类型
                            log.warn("Dameng database does not support MONTH_DAY_NANO interval type. Setting value to null.");
                            value = null;
                            break;
                        default:
                            log.warn("Unsupported interval unit: {}. Setting value to null.", intervalType.getUnit());
                            value = null;
                            break;
                    }
                    if (value != null && !(value instanceof String)) {
                        value = null;
                        log.warn("Unsupported Arrow Interval type: {}. set original value is null.", intervalType.getUnit());
                    }
                }
                params.add(value);
            }
        }

        return new DatabaseRecordWriter.SqlWithParams(sb.toString(), params);
    }

    /**
     * 检查表是否存在于数据库中。
     *
     * @param connection 数据库连接
     * @param tableName 要检查的表名
     * @return 如果表存在返回true，否则返回false
     */
    public static boolean checkTableExists(Connection connection, String tableName) {
        // 验证表名
        if (!IDENTIFIER_PATTERN.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            // 达梦SQL检查表是否存在 - 使用用户表视图
            rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME='" + tableName + "'"
            );
            if (rs.next() && rs.getInt(1) > 0) {
                log.info("Table {} exists in database", tableName);
                return true;
            }
            return false;
        } catch (SQLException e) {
            log.error("Error checking table existence: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("Error closing result set or statement", e);
            }
        }
    }


    /**
     * 转义SQL字符串，防止SQL注入。
     */
    private static String escapeString(String str) {
        return str.replace("'", "''");
    }

}
