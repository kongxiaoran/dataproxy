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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.*;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;
import org.secretflow.v1alpha1.common.Common;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit test class for DatabaseDoGetContext.
 * 
 * @author kongxiaoran
 * @date 2025/11/07
 */
@ExtendWith(MockitoExtension.class)
public class DatabaseDoGetContextTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private DatabaseMetaData mockDatabaseMetaData;

    @Mock
    private ResultSetMetaData mockResultSetMetaData;

    @Mock
    private ResultSet mockColumnsResultSet;

    private DatabaseConnectConfig dbConnectConfig;
    private Function<DatabaseConnectConfig, Connection> initDatabaseFunc;
    private DatabaseDoGetContext.BuildQuerySqlFunc<String, List<String>, String, String> buildQuerySqlFunc;
    private Function<String, ArrowType> jdbcType2ArrowType;

    @BeforeEach
    public void setUp() throws SQLException {
        dbConnectConfig = new DatabaseConnectConfig("user", "pass", "localhost:3306", "testdb");
        
        // Set initDatabaseFunc
        initDatabaseFunc = config -> mockConnection;
        
        // Set buildQuerySqlFunc
        buildQuerySqlFunc = (tableName, columns, partition) -> {
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(String.join(", ", columns));
            sql.append(" FROM ").append(tableName);
            if (partition != null && !partition.isEmpty()) {
                sql.append(" WHERE ").append(partition);
            }
            return sql.toString();
        };
        
        // Set jdbcType2ArrowType
        jdbcType2ArrowType = typeName -> {
            if (typeName == null || typeName.isEmpty()) {
                return ArrowType.Utf8.INSTANCE;
            }
            String upperType = typeName.toUpperCase();
            if (upperType.contains("INT")) {
                return new ArrowType.Int(32, true);
            } else if (upperType.contains("VARCHAR") || upperType.contains("CHAR")) {
                return ArrowType.Utf8.INSTANCE;
            } else if (upperType.contains("DECIMAL") || upperType.contains("NUMERIC")) {
                return new ArrowType.Decimal(10, 2, 128);
            } else {
                return ArrowType.Utf8.INSTANCE;
            }
        };
    }

    @Test
    public void testConstructor_WithTableQueryConfig() throws SQLException {
        // Prepare test data
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                "dt=20240101",
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build(),
                        Common.DataColumn.newBuilder().setName("name").setType("string").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        // Set mock behavior
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0, 0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(-1, -1);
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        assertNotNull(context);
        assertNotNull(context.getSchema());
        assertEquals("test_table", context.getTableName());
        assertNotNull(context.getResultSet());
        assertNotNull(context.getDatabaseMetaData());
    }

    @Test
    public void testConstructor_WithScqlCommandJobConfig() throws SQLException {
        ScqlCommandJobConfig sqlConfig = new ScqlCommandJobConfig(
                dbConnectConfig,
                "SELECT * FROM test_table"
        );

        // Set mock behavior - SQL query scenario
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
        when(mockResultSetMetaData.getColumnName(1)).thenReturn("id");
        when(mockResultSetMetaData.getColumnName(2)).thenReturn("name");
        when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("INT");
        when(mockResultSetMetaData.getColumnTypeName(2)).thenReturn("VARCHAR");
        when(mockResultSetMetaData.getPrecision(1)).thenReturn(0);
        when(mockResultSetMetaData.getPrecision(2)).thenReturn(0);
        when(mockResultSetMetaData.getScale(1)).thenReturn(-1);
        when(mockResultSetMetaData.getScale(2)).thenReturn(-1);
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                sqlConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        assertNotNull(context);
        assertNotNull(context.getSchema());
        assertNotNull(context.getResultSet());
    }

    @Test
    public void testConstructor_WithUnsupportedConfig() {
        // Create an unsupported config type
        DatabaseCommandConfig<?> unsupportedConfig = new DatabaseCommandConfig<Object>(
                dbConnectConfig, DatabaseTypeEnum.TABLE, new Object()
        ) {
            @Override
            public String taskRunSQL() {
                return "";
            }

            @Override
            public Schema getResultSchema() {
                return null;
            }
        };
        
        assertThrows(DataproxyException.class, () -> {
            new DatabaseDoGetContext(
                    unsupportedConfig,
                    initDatabaseFunc,
                    buildQuerySqlFunc,
                    jdbcType2ArrowType
            );
        });
    }

    @Test
    public void testConstructor_WithSQLException() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        // Mock SQLException
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenThrow(new SQLException("Database error"));
        
        assertThrows(DataproxyException.class, () -> {
            new DatabaseDoGetContext(
                    queryConfig,
                    initDatabaseFunc,
                    buildQuerySqlFunc,
                    jdbcType2ArrowType
            );
        });
    }

    @Test
    public void testGetTaskConfigs() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("id");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("INT");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(-1);
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        List<TaskConfig> taskConfigs = context.getTaskConfigs();
        assertNotNull(taskConfigs);
        assertEquals(1, taskConfigs.size());
        assertNotNull(taskConfigs.get(0));
    }

    @Test
    public void testClose() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("id");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("INT");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(-1);
        
        doNothing().when(mockResultSet).close();
        doNothing().when(mockStatement).close();
        doNothing().when(mockConnection).close();
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        assertDoesNotThrow(() -> {
            context.close();
        });
        
        verify(mockResultSet, times(1)).close();
        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testClose_WithSQLException() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("id");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("INT");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(-1);
        
        doThrow(new SQLException("Close error")).when(mockResultSet).close();
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        assertThrows(RuntimeException.class, () -> {
            context.close();
        });
    }

    @Test
    public void testClose_WithConnectionFailure() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("id").setType("int32").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("id");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("INT");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(-1);
        
        doNothing().when(mockResultSet).close();
        doNothing().when(mockStatement).close();
        doThrow(new SQLException("Connection close error")).when(mockConnection).close();
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                jdbcType2ArrowType
        );
        
        assertThrows(RuntimeException.class, () -> {
            context.close();
        });
    }

    @Test
    public void testInitArrowSchemaFromResultSet_WithDecimalType() throws SQLException {
        ScqlCommandJobConfig sqlConfig = new ScqlCommandJobConfig(
                dbConnectConfig,
                "SELECT * FROM test_table"
        );
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(mockResultSetMetaData.getColumnName(1)).thenReturn("price");
        when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("DECIMAL");
        when(mockResultSetMetaData.getPrecision(1)).thenReturn(10);
        when(mockResultSetMetaData.getScale(1)).thenReturn(2);
        
        // Update jdbcType2ArrowType to handle DECIMAL
        Function<String, ArrowType> decimalTypeFunc = typeName -> {
            if (typeName != null && typeName.toUpperCase().contains("DECIMAL")) {
                return new ArrowType.Decimal(10, 2, 128);
            }
            return ArrowType.Utf8.INSTANCE;
        };
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                sqlConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                decimalTypeFunc
        );
        
        assertNotNull(context.getSchema());
        assertEquals(1, context.getSchema().getFields().size());
        assertTrue(context.getSchema().getFields().get(0).getType() instanceof ArrowType.Decimal);
    }

    @Test
    public void testInitArrowSchemaFromColumns_WithTimeType() throws SQLException {
        DatabaseTableConfig tableConfig = new DatabaseTableConfig(
                "test_table",
                null,
                Arrays.asList(
                        Common.DataColumn.newBuilder().setName("created_at").setType("timestamp").build()
                )
        );
        
        DatabaseTableQueryConfig queryConfig = new DatabaseTableQueryConfig(dbConnectConfig, tableConfig);
        
        when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockDatabaseMetaData.getColumns(any(), any(), eq("test_table"), any()))
                .thenReturn(mockColumnsResultSet);
        when(mockColumnsResultSet.next()).thenReturn(true, false);
        when(mockColumnsResultSet.getString("COLUMN_NAME")).thenReturn("created_at");
        when(mockColumnsResultSet.getString("TYPE_NAME")).thenReturn("TIMESTAMP");
        when(mockColumnsResultSet.getInt("COLUMN_SIZE")).thenReturn(0);
        when(mockColumnsResultSet.getInt("DECIMAL_DIGITS")).thenReturn(3); // Millisecond precision
        
        // Update jdbcType2ArrowType to handle TIMESTAMP
        Function<String, ArrowType> timestampTypeFunc = typeName -> {
            if (typeName != null && typeName.toUpperCase().contains("TIMESTAMP")) {
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            }
            return ArrowType.Utf8.INSTANCE;
        };
        
        DatabaseDoGetContext context = new DatabaseDoGetContext(
                queryConfig,
                initDatabaseFunc,
                buildQuerySqlFunc,
                timestampTypeFunc
        );
        
        assertNotNull(context.getSchema());
        assertEquals(1, context.getSchema().getFields().size());
    }
}

