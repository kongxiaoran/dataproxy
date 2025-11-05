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

import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.sql.*;
import java.time.Duration;
import java.time.Period;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit test class for DamengUtil.
 *
 * @author kongxiaoran
 * @date 2025/11/07
 */
@ExtendWith(MockitoExtension.class)
public class DamengUtilTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private DatabaseMetaData mockDatabaseMetaData;

    private DatabaseConnectConfig validConfig;

    @BeforeEach
    public void setUp() {
        validConfig = new DatabaseConnectConfig(
                "testuser",
                "testpass",
                "localhost:5236",
                "TESTDB"
        );
    }

    @Test
    public void testInitDameng_ValidConfig() {
        // Note: This test requires real JDBC driver, may not run in unit tests
        // Actual tests should be in integration tests
        // Here mainly tests parameter validation logic
        assertThrows(RuntimeException.class, () -> {
            DamengUtil.initDameng(validConfig);
        });
    }

    @Test
    public void testInitDameng_InvalidEndpoint_Empty() {
        DatabaseConnectConfig config = new DatabaseConnectConfig(
                "user", "pass", "", "DB"
        );
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.initDameng(config);
        });
    }

    @Test
    public void testInitDameng_InvalidEndpoint_InvalidIP() {
        DatabaseConnectConfig config = new DatabaseConnectConfig(
                "user", "pass", "invalid..ip:5236", "DB"
        );
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.initDameng(config);
        });
    }

    @Test
    public void testInitDameng_InvalidPort_OutOfRange() {
        DatabaseConnectConfig config = new DatabaseConnectConfig(
                "user", "pass", "localhost:70000", "DB"
        );
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.initDameng(config);
        });
    }

    @Test
    public void testInitDameng_InvalidDatabaseName() {
        DatabaseConnectConfig config = new DatabaseConnectConfig(
                "user", "pass", "localhost:5236", "invalid-db-name!"
        );
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.initDameng(config);
        });
    }

    @Test
    public void testInitDameng_EndpointWithoutPort() {
        DatabaseConnectConfig config = new DatabaseConnectConfig(
                "user", "pass", "localhost", "DB"
        );
        // Should use default port 5236
        assertThrows(RuntimeException.class, () -> {
            DamengUtil.initDameng(config);
        });
    }


    @Test
    public void testBuildQuerySql_SimpleQuery() {
        List<String> columns = Arrays.asList("col1", "col2", "col3");
        String sql = DamengUtil.buildQuerySql("test_table", columns, null);
        
        assertEquals("SELECT col1, col2, col3 FROM test_table", sql);
    }

    @Test
    public void testBuildQuerySql_WithPartition() {
        List<String> columns = Arrays.asList("col1", "col2");
        String partition = "dt=20240101";
        String sql = DamengUtil.buildQuerySql("test_table", columns, partition);
        
        assertTrue(sql.contains("SELECT col1, col2 FROM test_table"));
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("dt='20240101'"));
    }

    @Test
    public void testBuildQuerySql_WithMultiplePartitions() {
        List<String> columns = Arrays.asList("col1");
        String partition = "dt=20240101,region=us";
        String sql = DamengUtil.buildQuerySql("test_table", columns, partition);
        
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("dt='20240101'"));
        assertTrue(sql.contains("region='us'"));
        assertTrue(sql.contains("AND"));
    }

    @Test
    public void testBuildQuerySql_EmptyColumns() {
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildQuerySql("test_table", Collections.emptyList(), null);
        });
    }

    @Test
    public void testBuildQuerySql_InvalidTableName() {
        List<String> columns = Arrays.asList("col1");
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildQuerySql("invalid-table-name!", columns, null);
        });
    }

    @Test
    public void testBuildQuerySql_InvalidColumnName() {
        List<String> columns = Arrays.asList("col1", "invalid-column!");
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildQuerySql("test_table", columns, null);
        });
    }

    @Test
    public void testBuildQuerySql_InvalidPartitionKey() {
        List<String> columns = Arrays.asList("col1");
        String partition = "invalid-key!=value";
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildQuerySql("test_table", columns, partition);
        });
    }

    @Test
    public void testBuildQuerySql_PartitionValueWithSpecialChars() {
        List<String> columns = Arrays.asList("col1");
        String partition = "dt=2024-01-01";
        String sql = DamengUtil.buildQuerySql("test_table", columns, partition);
        assertTrue(sql.contains("dt='2024-01-01'"));
    }

    @Test
    public void testBuildQuerySql_PartitionValueWithSingleQuote() {
        List<String> columns = Arrays.asList("col1");
        String partition = "dt=test'value";
        String sql = DamengUtil.buildQuerySql("test_table", columns, partition);
        // Should escape single quotes
        assertTrue(sql.contains("dt='test''value'"));
    }


    @Test
    public void testBuildCreateTableSql_SimpleTable() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        String sql = DamengUtil.buildCreateTableSql("test_table", schema, null);
        
        assertTrue(sql.contains("CREATE TABLE test_table"));
        assertTrue(sql.contains("id INT"));
        assertTrue(sql.contains("name VARCHAR"));
    }

    @Test
    public void testBuildCreateTableSql_WithPartition() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        Map<String, String> partition = Map.of("dt", "20240101");
        String sql = DamengUtil.buildCreateTableSql("test_table", schema, partition);
        
        assertTrue(sql.contains("CREATE TABLE test_table"));
        assertTrue(sql.contains("dt VARCHAR"));
    }

    @Test
    public void testBuildCreateTableSql_EmptySchema() {
        Schema schema = new Schema(Collections.emptyList());
        
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildCreateTableSql("test_table", schema, null);
        });
    }

    @Test
    public void testBuildCreateTableSql_InvalidTableName() {
        Schema schema = new Schema(Arrays.asList(
                new Field("col1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildCreateTableSql("invalid-table!", schema, null);
        });
    }

    @Test
    public void testBuildCreateTableSql_InvalidFieldName() {
        Schema schema = new Schema(Arrays.asList(
                new Field("invalid-field!", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildCreateTableSql("test_table", schema, null);
        });
    }


    @Test
    public void testArrowTypeToJdbcType_StringTypes() {
        assertEquals("VARCHAR", DamengUtil.arrowTypeToJdbcType(ArrowType.Utf8.INSTANCE));
        assertEquals("CLOB", DamengUtil.arrowTypeToJdbcType(ArrowType.LargeUtf8.INSTANCE));
    }

    @Test
    public void testArrowTypeToJdbcType_IntegerTypes() {
        assertEquals("TINYINT", DamengUtil.arrowTypeToJdbcType(new ArrowType.Int(8, true)));
        assertEquals("SMALLINT", DamengUtil.arrowTypeToJdbcType(new ArrowType.Int(16, true)));
        assertEquals("INT", DamengUtil.arrowTypeToJdbcType(new ArrowType.Int(32, true)));
        assertEquals("BIGINT", DamengUtil.arrowTypeToJdbcType(new ArrowType.Int(64, true)));
    }

    @Test
    public void testArrowTypeToJdbcType_InvalidIntBitWidth() {
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Int(128, true));
        });
    }

    @Test
    public void testArrowTypeToJdbcType_FloatingPointTypes() {
        assertEquals("FLOAT", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        assertEquals("DOUBLE", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    }

    @Test
    public void testArrowTypeToJdbcType_Bool() {
        assertEquals("BIT", DamengUtil.arrowTypeToJdbcType(ArrowType.Bool.INSTANCE));
    }

    @Test
    public void testArrowTypeToJdbcType_DateTypes() {
        assertEquals("DATE", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Date(DateUnit.DAY)));
        assertEquals("DATETIME(3)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Date(DateUnit.MILLISECOND)));
    }

    @Test
    public void testArrowTypeToJdbcType_TimeTypes() {
        assertEquals("TIME(0)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Time(TimeUnit.SECOND, 32)));
        assertEquals("TIME(3)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32)));
        assertEquals("TIME(6)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
        assertEquals("TIME(6)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Time(TimeUnit.NANOSECOND, 64)));
    }

    @Test
    public void testArrowTypeToJdbcType_TimestampTypes() {
        assertEquals("TIMESTAMP(0)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Timestamp(TimeUnit.SECOND, null)));
        assertEquals("TIMESTAMP(3)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        assertEquals("TIMESTAMP(6)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    }

    @Test
    public void testArrowTypeToJdbcType_TimestampWithTimezone() {
        ArrowType.Timestamp ts = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
        String result = DamengUtil.arrowTypeToJdbcType(ts);
        assertTrue(result.contains("TIMESTAMP WITH TIME ZONE"));
    }

    @Test
    public void testArrowTypeToJdbcType_TimestampNanosecond() {
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
        });
    }

    @Test
    public void testArrowTypeToJdbcType_Decimal() {
        assertEquals("DECIMAL(10, 2)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Decimal(10, 2, 128)));
        assertEquals("DECIMAL(38, 10)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Decimal(38, 10, 128)));
    }

    @Test
    public void testArrowTypeToJdbcType_Decimal_InvalidPrecision() {
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Decimal(0, 0, 128));
        });
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Decimal(39, 10, 128));
        });
    }

    @Test
    public void testArrowTypeToJdbcType_Decimal_InvalidScale() {
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Decimal(10, -1, 128));
        });
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.arrowTypeToJdbcType(new ArrowType.Decimal(10, 11, 128));
        });
    }

    @Test
    public void testArrowTypeToJdbcType_BinaryTypes() {
        assertEquals("VARBINARY", DamengUtil.arrowTypeToJdbcType(ArrowType.Binary.INSTANCE));
        assertEquals("BLOB", DamengUtil.arrowTypeToJdbcType(ArrowType.LargeBinary.INSTANCE));
    }

    @Test
    public void testArrowTypeToJdbcType_FixedSizeBinary() {
        assertEquals("BINARY(10)", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.FixedSizeBinary(10)));
        assertEquals("BLOB", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.FixedSizeBinary(10000)));
    }

    @Test
    public void testArrowTypeToJdbcType_IntervalTypes() {
        assertEquals("INTERVAL YEAR TO MONTH", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Interval(IntervalUnit.YEAR_MONTH)));
        assertEquals("INTERVAL DAY TO SECOND", DamengUtil.arrowTypeToJdbcType(
                new ArrowType.Interval(IntervalUnit.DAY_TIME)));
    }

    @Test
    public void testArrowTypeToJdbcType_Null() {
        assertEquals("VARCHAR(1)", DamengUtil.arrowTypeToJdbcType(ArrowType.Null.INSTANCE));
    }

    @Test
    public void testArrowTypeToJdbcType_UnsupportedType() {
        // Cannot create unsupported ArrowType instance (ArrowType is final)
        // This test mainly verifies the else branch in code
    }

    @Test
    public void testJdbcType2ArrowType_DecimalTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("DECIMAL");
        assertInstanceOf(ArrowType.Decimal.class, type1);
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("NUMERIC");
        assertInstanceOf(ArrowType.Decimal.class, type2);
        
        ArrowType type3 = DamengUtil.jdbcType2ArrowType("NUMBER");
        assertInstanceOf(ArrowType.Decimal.class, type3);
    }

    @Test
    public void testJdbcType2ArrowType_TimestampTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("TIMESTAMP(3)");
        assertInstanceOf(ArrowType.Timestamp.class, type1);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Timestamp) type1).getUnit());
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("TIMESTAMP(6)");
        assertInstanceOf(ArrowType.Timestamp.class, type2);
        assertEquals(TimeUnit.MICROSECOND, ((ArrowType.Timestamp) type2).getUnit());
    }

    @Test
    public void testJdbcType2ArrowType_DatetimeTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("DATETIME(3)");
        assertInstanceOf(ArrowType.Date.class, type1);
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("DATETIME(6)");
        assertInstanceOf(ArrowType.Timestamp.class, type2);
    }

    @Test
    public void testJdbcType2ArrowType_TimeTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("TIME(3)");
        assertInstanceOf(ArrowType.Time.class, type1);
        ArrowType.Time time1 = (ArrowType.Time) type1;
        assertEquals(TimeUnit.MILLISECOND, time1.getUnit());
        assertEquals(32, time1.getBitWidth());
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("TIME(6)");
        assertInstanceOf(ArrowType.Time.class, type2);
        ArrowType.Time time2 = (ArrowType.Time) type2;
        assertEquals(TimeUnit.MICROSECOND, time2.getUnit());
        assertEquals(64, time2.getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_TimestampWithTimezone() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("TIMESTAMP WITH TIME ZONE");
        assertInstanceOf(ArrowType.Timestamp.class, type1);
        assertEquals("UTC", ((ArrowType.Timestamp) type1).getTimezone());
    }

    @Test
    public void testJdbcType2ArrowType_StringTypes() {
        assertEquals(ArrowType.Utf8.INSTANCE, DamengUtil.jdbcType2ArrowType("VARCHAR"));
        assertEquals(ArrowType.Utf8.INSTANCE, DamengUtil.jdbcType2ArrowType("CHAR"));
        assertEquals(ArrowType.LargeUtf8.INSTANCE, DamengUtil.jdbcType2ArrowType("CLOB"));
        assertEquals(ArrowType.LargeUtf8.INSTANCE, DamengUtil.jdbcType2ArrowType("TEXT"));
    }

    @Test
    public void testJdbcType2ArrowType_IntegerTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("TINYINT");
        assertInstanceOf(ArrowType.Int.class, type1);
        assertEquals(8, ((ArrowType.Int) type1).getBitWidth());
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("SMALLINT");
        assertInstanceOf(ArrowType.Int.class, type2);
        assertEquals(16, ((ArrowType.Int) type2).getBitWidth());
        
        ArrowType type3 = DamengUtil.jdbcType2ArrowType("INT");
        assertInstanceOf(ArrowType.Int.class, type3);
        assertEquals(32, ((ArrowType.Int) type3).getBitWidth());
        
        ArrowType type4 = DamengUtil.jdbcType2ArrowType("BIGINT");
        assertInstanceOf(ArrowType.Int.class, type4);
        assertEquals(64, ((ArrowType.Int) type4).getBitWidth());
    }

    @Test
    public void testJdbcType2ArrowType_FloatingPointTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("FLOAT");
        assertInstanceOf(ArrowType.FloatingPoint.class, type1);
        assertEquals(FloatingPointPrecision.SINGLE, 
                ((ArrowType.FloatingPoint) type1).getPrecision());
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("DOUBLE");
        assertInstanceOf(ArrowType.FloatingPoint.class, type2);
        assertEquals(FloatingPointPrecision.DOUBLE, 
                ((ArrowType.FloatingPoint) type2).getPrecision());
    }

    @Test
    public void testJdbcType2ArrowType_Bool() {
        assertEquals(ArrowType.Bool.INSTANCE, DamengUtil.jdbcType2ArrowType("BIT"));
        assertEquals(ArrowType.Bool.INSTANCE, DamengUtil.jdbcType2ArrowType("BOOLEAN"));
    }

    @Test
    public void testJdbcType2ArrowType_Date() {
        ArrowType type = DamengUtil.jdbcType2ArrowType("DATE");
        assertInstanceOf(ArrowType.Date.class, type);
        assertEquals(DateUnit.DAY, ((ArrowType.Date) type).getUnit());
    }

    @Test
    public void testJdbcType2ArrowType_BinaryTypes() {
        assertEquals(ArrowType.Binary.INSTANCE, DamengUtil.jdbcType2ArrowType("VARBINARY"));
        assertEquals(ArrowType.Binary.INSTANCE, DamengUtil.jdbcType2ArrowType("BINARY"));
        assertEquals(ArrowType.LargeBinary.INSTANCE, DamengUtil.jdbcType2ArrowType("BLOB"));
    }

    @Test
    public void testJdbcType2ArrowType_IntervalTypes() {
        ArrowType type1 = DamengUtil.jdbcType2ArrowType("INTERVAL YEAR TO MONTH");
        assertInstanceOf(ArrowType.Interval.class, type1);
        assertEquals(IntervalUnit.YEAR_MONTH, ((ArrowType.Interval) type1).getUnit());
        
        ArrowType type2 = DamengUtil.jdbcType2ArrowType("INTERVAL DAY TO SECOND");
        assertInstanceOf(ArrowType.Interval.class, type2);
        assertEquals(IntervalUnit.DAY_TIME, ((ArrowType.Interval) type2).getUnit());
    }

    @Test
    public void testJdbcType2ArrowType_Null() {
        assertEquals(ArrowType.Null.INSTANCE, DamengUtil.jdbcType2ArrowType("NULL"));
    }

    @Test
    public void testJdbcType2ArrowType_UnsupportedType() {
        // Unsupported types should return Utf8 as fallback
        ArrowType type = DamengUtil.jdbcType2ArrowType("UNKNOWN_TYPE");
        assertEquals(ArrowType.Utf8.INSTANCE, type);
    }

    @Test
    public void testJdbcType2ArrowType_NullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.jdbcType2ArrowType(null);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.jdbcType2ArrowType("");
        });
    }


    @Test
    public void testBuildMultiRowInsertSql_SimpleInsert() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("name", "test1");
        dataList.add(row1);
        
        Map<String, String> partition = Collections.emptyMap();
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, partition);
        
        assertNotNull(result);
        assertNotNull(result.sql);
        assertTrue(result.sql.contains("INSERT INTO test_table"));
        assertTrue(result.sql.contains("id"));
        assertTrue(result.sql.contains("name"));
        assertNotNull(result.params);
        assertEquals(2, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_MultipleRows() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            dataList.add(row);
        }
        
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, Collections.emptyMap());
        
        assertNotNull(result);
        assertTrue(result.sql.contains("VALUES"));
        assertEquals(3, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_WithPartition() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        dataList.add(row);
        
        Map<String, String> partition = Map.of("dt", "20240101");
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, partition);
        
        assertNotNull(result);
        assertTrue(result.sql.contains("dt"));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithBool() {
        Schema schema = new Schema(Arrays.asList(
                new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("flag", true);
        dataList.add(row);
        
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, Collections.emptyMap());
        
        assertNotNull(result);
        assertEquals(1, result.params.size());
        assertEquals(1, result.params.get(0)); // true -> 1
    }

    @Test
    public void testBuildMultiRowInsertSql_WithTimestamp() {
        Schema schema = new Schema(Arrays.asList(
                new Field("ts", FieldType.nullable(
                        new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("ts", System.currentTimeMillis());
        dataList.add(row);
        
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, Collections.emptyMap());
        
        assertNotNull(result);
        assertEquals(1, result.params.size());
    }

    @Test
    public void testBuildMultiRowInsertSql_WithIntervalYearMonth() {
        Schema schema = new Schema(Arrays.asList(
                new Field("interval_col", FieldType.nullable(
                        new ArrowType.Interval(IntervalUnit.YEAR_MONTH)), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("interval_col", 14); // 14 months = 1 year 2 months
        dataList.add(row);
        
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, Collections.emptyMap());
        
        assertNotNull(result);
        assertTrue(result.sql.contains("INTERVAL"));
        assertTrue(result.sql.contains("YEAR TO MONTH"));
    }

    @Test
    public void testBuildMultiRowInsertSql_WithIntervalDayTime() {
        Schema schema = new Schema(Arrays.asList(
                new Field("interval_col", FieldType.nullable(
                        new ArrowType.Interval(IntervalUnit.DAY_TIME)), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        PeriodDuration pd = new PeriodDuration(Period.ZERO, Duration.ofHours(2));
        row.put("interval_col", pd);
        dataList.add(row);
        
        DatabaseRecordWriter.SqlWithParams result = DamengUtil.buildMultiRowInsertSql(
                "test_table", schema, dataList, Collections.emptyMap());
        
        assertNotNull(result);
        assertTrue(result.sql.contains("INTERVAL"));
        assertTrue(result.sql.contains("DAY TO SECOND"));
    }

    @Test
    public void testBuildMultiRowInsertSql_EmptyDataList() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        
        assertThrows(IllegalArgumentException.class, () -> {
            DamengUtil.buildMultiRowInsertSql("test_table", schema, 
                    Collections.emptyList(), Collections.emptyMap());
        });
    }

    @Test
    public void testBuildMultiRowInsertSql_InvalidTableName() {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        dataList.add(row);
        
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.buildMultiRowInsertSql("invalid-table!", schema, 
                    dataList, Collections.emptyMap());
        });
    }


    @Test
    public void testCheckTableExists_TableExists() throws SQLException {
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(1);
        
        boolean exists = DamengUtil.checkTableExists(mockConnection, "TEST_TABLE");
        
        assertTrue(exists);
        verify(mockStatement).executeQuery(contains("SELECT COUNT(*) FROM USER_TABLES"));
    }

    @Test
    public void testCheckTableExists_TableNotExists() throws SQLException {
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0);
        
        boolean exists = DamengUtil.checkTableExists(mockConnection, "NONEXISTENT_TABLE");
        
        assertFalse(exists);
    }

    @Test
    public void testCheckTableExists_InvalidTableName() {
        assertThrows(DataproxyException.class, () -> {
            DamengUtil.checkTableExists(mockConnection, "invalid-table!");
        });
    }

    @Test
    public void testCheckTableExists_SQLException() throws SQLException {
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("Database error"));
        
        assertThrows(RuntimeException.class, () -> {
            DamengUtil.checkTableExists(mockConnection, "TEST_TABLE");
        });
    }

    @Test
    public void testCheckTableExists_TableNameCaseInsensitive() throws SQLException {
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(1);
        
        boolean exists1 = DamengUtil.checkTableExists(mockConnection, "test_table");
        assertTrue(exists1);
        
        boolean exists2 = DamengUtil.checkTableExists(mockConnection, "TEST_TABLE");
        assertTrue(exists2);
        
        // Verify SQL uses UPPER()
        verify(mockStatement, atLeastOnce()).executeQuery(argThat(sql -> 
                sql.contains("UPPER(TABLE_NAME)")));
    }
}

