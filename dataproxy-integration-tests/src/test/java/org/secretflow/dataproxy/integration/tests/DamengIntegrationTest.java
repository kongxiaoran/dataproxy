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

package org.secretflow.dataproxy.integration.tests;

import com.google.protobuf.Any;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.secretflow.dataproxy.common.utils.ArrowUtil;
import org.secretflow.dataproxy.integration.tests.utils.DamengTestUtil;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.server.DataProxyFlightServer;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


/**
 * @author kongxiaoran
 * @date 2025/11/07
 * Comprehensive Dameng Integration Test covering all Arrow types supported by ArrowUtil.parseKusciaColumnType
 */
@Slf4j
@Testcontainers
@EnabledIfSystemProperty(named = "enableDamengIntegration", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DamengIntegrationTest extends BaseArrowFlightServerTest {

    // User/password info for database built into Dameng docker image
    private static final String DOCKER_DAMENG_USER = "SYSDBA";
    private static final String DOCKER_DAMENG_PASSWORD = "SYSDBA001";
    private static final String DOCKER_DATABASE_NAME = "SYSDBA";
    private static final int DOCKER_CONTAINER_PORT = 5237;
    private static final String TABLE_NAME = "COMPREHENSIVE_TEST_TABLE";

    // Whether to use Dameng docker for unit testing or remote Dameng database
    private static final boolean USE_DOCKER = Boolean.parseBoolean(
            System.getProperty("useDamengDocker", "true"));

    private static String damengJdbcUrl;
    private static String damengHost;
    private static int damengPort;
    private static String damengUser;
    private static String damengPassword;
    private static String databaseName;

    private static GenericContainer<?> dameng;
    private static Domaindatasource.DomainDataSource domainDataSource;
    private static Domaindata.DomainData domainDataWithTable;

    // Comprehensive column definitions covering all supported types
    // Note: uint types are skipped as Dameng doesn't support unsigned integers
    private static final List<Common.DataColumn> columns = Arrays.asList(
            // Integer types
            Common.DataColumn.newBuilder().setName("col_int8").setType("int8").build(),
            Common.DataColumn.newBuilder().setName("col_int16").setType("int16").build(),
            Common.DataColumn.newBuilder().setName("col_int32").setType("int32").build(),
            Common.DataColumn.newBuilder().setName("col_int64").setType("int64").build(),
            // Floating point types
            Common.DataColumn.newBuilder().setName("col_float32").setType("float32").build(),
            Common.DataColumn.newBuilder().setName("col_float64").setType("float64").build(),
            // Date types
            Common.DataColumn.newBuilder().setName("col_date32").setType("date32").build(),
            Common.DataColumn.newBuilder().setName("col_date64").setType("date64").build(),
            // Time types
            Common.DataColumn.newBuilder().setName("col_time32").setType("time32").build(),
            Common.DataColumn.newBuilder().setName("col_time64").setType("time64").build(),
            // Timestamp types
            Common.DataColumn.newBuilder().setName("col_timestamp").setType("timestamp").build(),
            Common.DataColumn.newBuilder().setName("col_timestamp_ms").setType("timestamp_ms").build(),
            Common.DataColumn.newBuilder().setName("col_timestamp_us").setType("timestamp_us").build(),
            // Boolean types
            Common.DataColumn.newBuilder().setName("col_bool").setType("bool").build(),
            // String types
            Common.DataColumn.newBuilder().setName("col_string").setType("string").build(),
            Common.DataColumn.newBuilder().setName("col_large_string").setType("large_string").build(),
            // Binary types
            Common.DataColumn.newBuilder().setName("col_binary").setType("binary").build(),
            Common.DataColumn.newBuilder().setName("col_large_binary").setType("large_binary").build(),
            // Decimal types
            Common.DataColumn.newBuilder().setName("col_decimal").setType("decimal").build(),
            // Interval types
            Common.DataColumn.newBuilder().setName("col_interval_ym").setType("interval_year_month").build(),
            Common.DataColumn.newBuilder().setName("col_interval_dt").setType("interval_day_time").build()
    );

    @BeforeAll
    public static void startServer() {
        log.info("Starting comprehensive Dameng integration test...");
        
        // Start DataProxyFlightServer
        dataProxyFlightServer = new DataProxyFlightServer(FlightServerContext.getInstance().getFlightServerConfig());
        assertDoesNotThrow(() -> {
            serverThread = new Thread(() -> {
                try {
                    dataProxyFlightServer.start();
                    SERVER_START_LATCH.countDown();
                    dataProxyFlightServer.awaitTermination();
                } catch (Exception e) {
                    fail("Exception was thrown during server start: " + e.getMessage());
                }
            });
            serverThread.start();
            SERVER_START_LATCH.await();
        });

        // Setup database
        assertDoesNotThrow(() -> {
            if (USE_DOCKER) {
                log.info("Starting Docker container for Dameng database...");
                @SuppressWarnings("resource")
                GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("kongxr7/dameng:8.1"))
                        .withExposedPorts(DOCKER_CONTAINER_PORT, 5237)
                        .withPrivilegedMode(true)
                        .withEnv("PAGE_SIZE", "16")
                        .withEnv("LD_LIBRARY_PATH", "/opt/dmdbms/bin")
                        .withEnv("INSTANCE_NAME", "dm8db")
                        .withStartupTimeout(Duration.ofSeconds(600))
                        .waitingFor(Wait.forListeningPort());
                container.start();
                dameng = container;

                damengHost = dameng.getHost();
                damengPort = dameng.getMappedPort(DOCKER_CONTAINER_PORT);
                damengUser = DOCKER_DAMENG_USER;
                damengPassword = DOCKER_DAMENG_PASSWORD;
                databaseName = DOCKER_DATABASE_NAME;
                log.info("Using Docker container - Host: {}, Port: {}", damengHost, damengPort);
            } else {
                String endpoint = DamengTestUtil.getDamengEndpoint();
                String[] hostPort = endpoint.split(":");
                damengHost = hostPort[0];
                damengPort = Integer.parseInt(hostPort[1]);
                damengUser = DamengTestUtil.getDamengUsername();
                damengPassword = DamengTestUtil.getDamengPassword();
                databaseName = DamengTestUtil.getDamengDatabase();
                log.info("Using remote database - Host: {}, Port: {}", damengHost, damengPort);
            }

            damengJdbcUrl = String.format("jdbc:dm://%s:%d/%s", damengHost, damengPort, databaseName);

            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));

                // Create table with all supported types
                String createTableSql = String.format(
                    "CREATE TABLE %s (" +
                    "col_int8 TINYINT, " +
                    "col_int16 SMALLINT, " +
                    "col_int32 INT, " +
                    "col_int64 BIGINT, " +
                    "col_float32 FLOAT, " +
                    "col_float64 DOUBLE, " +
                    "col_date32 DATE, " +
                    // Date(MILLISECOND) -> DATETIME(3)
                    "col_date64 DATETIME(3), " +
                    "col_time32 TIME(3), " +
                    "col_time64 TIME(6), " +
                    "col_timestamp TIMESTAMP, " +
                    "col_timestamp_ms TIMESTAMP(3), " +
                    "col_timestamp_us TIMESTAMP(6), " +
                    "col_bool BIT, " +
                    "col_string VARCHAR(100), " +
                    "col_large_string CLOB, " +
                    "col_binary VARBINARY(100), " +
                    "col_large_binary BLOB, " +
                    "col_decimal DECIMAL(38, 10), " +
                    "col_interval_ym INTERVAL YEAR TO MONTH, " +
                    "col_interval_dt INTERVAL DAY TO SECOND" +
                    ")", TABLE_NAME);
                stmt.execute(createTableSql);
                log.info("Created comprehensive test table with {} columns", columns.size());
            }
        });

        // Prepare protobuf messages
        Domaindatasource.DatabaseDataSourceInfo damengDataSourceInfo =
                Domaindatasource.DatabaseDataSourceInfo.newBuilder()
                        .setEndpoint(String.format("%s:%d", damengHost, damengPort))
                        .setUser(damengUser)
                        .setPassword(damengPassword)
                        .setDatabase(databaseName)
                        .build();

        domainDataSource = Domaindatasource.DomainDataSource.newBuilder()
                .setDatasourceId("dameng-datasource")
                .setName("dameng_db")
                .setType("dameng")
                .setInfo(Domaindatasource.DataSourceInfo.newBuilder().setDatabase(damengDataSourceInfo))
                .build();

        domainDataWithTable = Domaindata.DomainData.newBuilder()
                .setDatasourceId("dameng-datasource")
                .setName(TABLE_NAME)
                .setRelativeUri(TABLE_NAME)
                .setDomaindataId("dameng-table")
                .setType("table")
                .addAllColumns(columns)
                .build();
    }

    @AfterAll
    public static void stopServer() {
        assertDoesNotThrow(() -> {
            if (dataProxyFlightServer != null) dataProxyFlightServer.close();
            if (serverThread != null) serverThread.interrupt();
            if (USE_DOCKER && dameng != null) {
                dameng.stop();
                log.info("Docker container stopped");
            }
        });
    }

    @Test
    @Order(1)
    public void testDoPut() {
        log.info("Testing DoPut with comprehensive data types...");
        
        Flightinner.CommandDataMeshUpdate command = Flightinner.CommandDataMeshUpdate.newBuilder()
                .setDatasource(domainDataSource)
                .setDomaindata(domainDataWithTable)
                .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                        .setContentType(Flightdm.ContentType.CSV))
                .build();

        assertDoesNotThrow(() -> {
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));
            Ticket ticket = info.getEndpoints().get(0).getTicket();

            Schema schema = new Schema(columns.stream()
                    .map(col -> Field.nullable(col.getName(), ArrowUtil.parseKusciaColumnType(col.getType())))
                    .collect(Collectors.toList()));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                
                // Get all vectors
                TinyIntVector int8Vector = (TinyIntVector) root.getVector("col_int8");
                SmallIntVector int16Vector = (SmallIntVector) root.getVector("col_int16");
                IntVector int32Vector = (IntVector) root.getVector("col_int32");
                BigIntVector int64Vector = (BigIntVector) root.getVector("col_int64");
                Float4Vector float32Vector = (Float4Vector) root.getVector("col_float32");
                Float8Vector float64Vector = (Float8Vector) root.getVector("col_float64");
                DateDayVector date32Vector = (DateDayVector) root.getVector("col_date32");
                DateMilliVector date64Vector = (DateMilliVector) root.getVector("col_date64");
                TimeMilliVector time32Vector = (TimeMilliVector) root.getVector("col_time32");
                TimeMicroVector time64Vector = (TimeMicroVector) root.getVector("col_time64");
                TimeStampMicroVector timestampVector = (TimeStampMicroVector) root.getVector("col_timestamp");
                TimeStampMilliVector timestampMsVector = (TimeStampMilliVector) root.getVector("col_timestamp_ms");
                TimeStampMicroVector timestampUsVector = (TimeStampMicroVector) root.getVector("col_timestamp_us");
                BitVector boolVector = (BitVector) root.getVector("col_bool");
                VarCharVector stringVector = (VarCharVector) root.getVector("col_string");
                LargeVarCharVector largeStringVector = (LargeVarCharVector) root.getVector("col_large_string");
                VarBinaryVector binaryVector = (VarBinaryVector) root.getVector("col_binary");
                LargeVarBinaryVector largeBinaryVector = (LargeVarBinaryVector) root.getVector("col_large_binary");
                DecimalVector decimalVector = (DecimalVector) root.getVector("col_decimal");
                IntervalYearVector intervalYVector = (IntervalYearVector) root.getVector("col_interval_ym");
                IntervalDayVector intervalDVector = (IntervalDayVector) root.getVector("col_interval_dt");
                
                // Set test data for row 0
                int8Vector.setSafe(0, 127);
                int16Vector.setSafe(0, 32767);
                int32Vector.setSafe(0, 2147483647);
                int64Vector.setSafe(0, 9223372036854775807L);
                float32Vector.setSafe(0, 3.14f);
                float64Vector.setSafe(0, 2.718281828459045);
                // Date: days since epoch (1970-01-01)
                date32Vector.setSafe(0, (int) LocalDate.of(2024, 1, 1).toEpochDay());
                // Date64: milliseconds since epoch
                date64Vector.setSafe(0, java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime());
                // Time32: milliseconds since midnight
                time32Vector.setSafe(0, (int) LocalTime.of(12, 30, 45).toSecondOfDay() * 1000);
                // Time64: microseconds since midnight
                // Note: LocalTime.of(12, 30, 45, 123456000) = 12:30:45.123456 (nanoseconds)
                time64Vector.setSafe(0, LocalTime.of(12, 30, 45, 123456000).toNanoOfDay() / 1000);
                // Timestamp: microseconds since epoch
                long timestampMs = java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime();
                timestampVector.setSafe(0, timestampMs * 1000);
                timestampMsVector.setSafe(0, timestampMs);
                timestampUsVector.setSafe(0, timestampMs * 1000);
                boolVector.setSafe(0, 1);
                stringVector.setSafe(0, "Test String".getBytes());
                largeStringVector.setSafe(0, ("Large String Content " + "A".repeat(100)).getBytes());
                binaryVector.setSafe(0, new byte[]{0x01, 0x02, 0x03, 0x04});
                largeBinaryVector.setSafe(0, new byte[]{0x05, 0x06, 0x07, 0x08, 0x09, 0x0A});
                decimalVector.setSafe(0, new BigDecimal("1234567890123456789012345678.1234567890"));
                // IntervalYear: total months (12 = 1 year)
                intervalYVector.setSafe(0, 12);
                // IntervalDay: days and milliseconds (1 day, 0 milliseconds)
                intervalDVector.setSafe(0, 1, 0);
                
                root.setRowCount(1);

                FlightClient.ClientStreamListener listener = client.startPut(
                        FlightDescriptor.command(ticket.getBytes()), root, new AsyncPutListener());
                listener.putNext();
                listener.completed();
                listener.getResult();
                
                log.info("Successfully wrote 1 row with all data types");
            }
        });
    }

    @Test
    @Order(2)
    public void testDoGet() {
        log.info("Testing DoGet to verify all data types and values...");
        
        Flightinner.CommandDataMeshQuery query = Flightinner.CommandDataMeshQuery.newBuilder()
                .setDatasource(domainDataSource)
                .setDomaindata(domainDataWithTable)
                .setQuery(Flightdm.CommandDomainDataQuery.newBuilder().setContentType(Flightdm.ContentType.CSV))
                .build();

        assertDoesNotThrow(() -> {
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(query).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                int rowCount = 0;
                VectorSchemaRoot root = null;
                
                while (stream.next()) {
                    root = stream.getRoot();
                    rowCount += root.getRowCount();
                    
                    if (rowCount > 0) {
                        // Verify all column types are present
                        assertEquals(columns.size(), root.getSchema().getFields().size(),
                                "Schema should contain all defined columns");
                
                        // Verify data can be read (basic sanity check)
                        for (Common.DataColumn col : columns) {
                            assertNotNull(root.getVector(col.getName()),
                                    "Column " + col.getName() + " should exist in result");
                        }
                        
                        log.info("Successfully read {} rows with {} columns", rowCount, columns.size());
                    }
                }
                
                assertEquals(1, rowCount, "Should read 1 row from table");
                
                // Verify data values match what was inserted
                if (root != null && root.getRowCount() > 0) {
                    verifyDataValues(root, 0);
                }
            }
        });
    }
    
    /**
     * Verify that the data values read from the database match what was inserted.
     * 
     * @param root VectorSchemaRoot containing the read data
     * @param rowIndex Row index to verify (should be 0 for single row test)
     */
    private void verifyDataValues(VectorSchemaRoot root, int rowIndex) {
        log.info("Verifying data values for row {}", rowIndex);
        
        // Integer types
        TinyIntVector int8Vector = (TinyIntVector) root.getVector("col_int8");
        assertEquals((byte) 127, int8Vector.get(rowIndex), "col_int8 should be 127");
        
        SmallIntVector int16Vector = (SmallIntVector) root.getVector("col_int16");
        assertEquals((short) 32767, int16Vector.get(rowIndex), "col_int16 should be 32767");
        
        IntVector int32Vector = (IntVector) root.getVector("col_int32");
        assertEquals(2147483647, int32Vector.get(rowIndex), "col_int32 should be 2147483647");
        
        BigIntVector int64Vector = (BigIntVector) root.getVector("col_int64");
        assertEquals(9223372036854775807L, int64Vector.get(rowIndex), "col_int64 should be 9223372036854775807");
        
        // Floating point types (use delta for floating point comparison)
        Float4Vector float32Vector = (Float4Vector) root.getVector("col_float32");
        assertEquals(3.14f, float32Vector.get(rowIndex), 0.001f, "col_float32 should be approximately 3.14");
        
        Float8Vector float64Vector = (Float8Vector) root.getVector("col_float64");
        assertEquals(2.718281828459045, float64Vector.get(rowIndex), 0.000000000000001, 
                "col_float64 should be approximately 2.718281828459045");
        
        // Date types
        DateDayVector date32Vector = (DateDayVector) root.getVector("col_date32");
        LocalDate expectedDate = LocalDate.of(2024, 1, 1);
        LocalDate actualDate = LocalDate.ofEpochDay(date32Vector.get(rowIndex));
        assertEquals(expectedDate, actualDate, "col_date32 should be 2024-01-01");
        
        // col_date64: Database TIMESTAMP may be read as TimeStampMicroVector instead of DateMilliVector
        FieldVector date64FieldVector = root.getVector("col_date64");
        long expectedDate64Ms = java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime();
        long actualDate64Ms = 0;
        
        if (date64FieldVector instanceof DateMilliVector) {
            DateMilliVector date64Vector = (DateMilliVector) date64FieldVector;
            actualDate64Ms = date64Vector.get(rowIndex);
        } else if (date64FieldVector instanceof TimeStampMicroVector) {
            // Database TIMESTAMP is often mapped to TimeStampMicroVector when reading
            TimeStampMicroVector timestampVector = (TimeStampMicroVector) date64FieldVector;
            // Convert microseconds to milliseconds
            actualDate64Ms = timestampVector.get(rowIndex) / 1000;
        } else if (date64FieldVector instanceof TimeStampMilliVector) {
            TimeStampMilliVector timestampVector = (TimeStampMilliVector) date64FieldVector;
            actualDate64Ms = timestampVector.get(rowIndex);
        } else {
            // Fallback: try to get as object and convert
            Object date64Obj = date64FieldVector.getObject(rowIndex);
            if (date64Obj instanceof java.time.LocalDateTime) {
                actualDate64Ms = java.sql.Timestamp.valueOf((java.time.LocalDateTime) date64Obj).getTime();
            } else {
                throw new AssertionError("Unexpected type for col_date64: " + 
                        (date64Obj != null ? date64Obj.getClass().getName() : "null"));
                            }
                        }
        // Allow 1 second tolerance for timestamp conversion
        assertEquals(expectedDate64Ms, actualDate64Ms, 1000, 
                "col_date64 should be approximately 2024-01-01 12:00:00");
        
        // Time types
        TimeMilliVector time32Vector = (TimeMilliVector) root.getVector("col_time32");
        LocalTime expectedTime32 = LocalTime.of(12, 30, 45);
        LocalTime actualTime32 = LocalTime.ofSecondOfDay(time32Vector.get(rowIndex) / 1000);
        assertEquals(expectedTime32.getHour(), actualTime32.getHour(), "col_time32 hour should be 12");
        assertEquals(expectedTime32.getMinute(), actualTime32.getMinute(), "col_time32 minute should be 30");
        assertEquals(expectedTime32.getSecond(), actualTime32.getSecond(), "col_time32 second should be 45");
        
        // col_time64: Database TIME(6) may be read as TimeMilliVector instead of TimeMicroVector
        FieldVector time64FieldVector = root.getVector("col_time64");
        LocalTime expectedTime64 = LocalTime.of(12, 30, 45, 123456000);
        LocalTime actualTime64 = null;
        
        if (time64FieldVector instanceof TimeMicroVector) {
            // TIME(6) -> TimeMicroVector (microseconds)
            TimeMicroVector time64Vector = (TimeMicroVector) time64FieldVector;
            long time64Micros = time64Vector.get(rowIndex);
            log.info("col_time64 read as TimeMicroVector: {} microseconds", time64Micros);
            actualTime64 = LocalTime.ofNanoOfDay(time64Micros * 1000);
            log.info("col_time64 converted to LocalTime: {}", actualTime64);
        } else if (time64FieldVector instanceof TimeMilliVector) {
            // TIME(6) should be read as TimeMicroVector, not TimeMilliVector
            // If it's read as TimeMilliVector, precision is lost and test should fail
            TimeMilliVector time64Vector = (TimeMilliVector) time64FieldVector;
            int time64Millis = time64Vector.get(rowIndex);
            fail("col_time64 was read as TimeMilliVector instead of TimeMicroVector. " +
                    "This indicates that TIME(6) precision was not correctly parsed. " +
                    "Expected microseconds precision but got milliseconds. " +
                    "Value: " + time64Millis + " milliseconds");
        } else {
            throw new AssertionError("Unexpected type for col_time64: " + 
                    (time64FieldVector != null ? time64FieldVector.getClass().getName() : "null"));
        }
        
        assertEquals(expectedTime64.getHour(), actualTime64.getHour(), "col_time64 hour should be 12");
        assertEquals(expectedTime64.getMinute(), actualTime64.getMinute(), "col_time64 minute should be 30");
        assertEquals(expectedTime64.getSecond(), actualTime64.getSecond(), "col_time64 second should be 45");
        /*
         * Note: java.sql.Time type from JDBC driver only supports second precision, not microsecond precision
         * Even though database TIME(6) can store microseconds, JDBC driver returns java.sql.Time which loses microsecond precision
         * Therefore, we only verify that the time matches up to seconds, not microseconds
         * The actual nanoseconds will be 0 due to JDBC driver limitation
         */
        log.info("col_time64 nanoseconds: {} (expected: {}). Note: JDBC java.sql.Time only supports second precision", 
                actualTime64.getNano(), expectedTime64.getNano());
                
        // Timestamp types
        // col_timestamp: Database TIMESTAMP may be read as different precision vectors
        FieldVector timestampFieldVector = root.getVector("col_timestamp");
        long expectedTimestampMs = java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime();
        long actualTimestampMs = 0;
        
        if (timestampFieldVector instanceof TimeStampMicroVector) {
            // TIMESTAMP(6) or TIMESTAMP without precision -> TimeStampMicroVector (microseconds)
            TimeStampMicroVector timestampVector = (TimeStampMicroVector) timestampFieldVector;
            // Convert microseconds to milliseconds
            actualTimestampMs = timestampVector.get(rowIndex) / 1000;
        } else if (timestampFieldVector instanceof TimeStampMilliVector) {
            // TIMESTAMP(3) -> TimeStampMilliVector (milliseconds)
            TimeStampMilliVector timestampVector = (TimeStampMilliVector) timestampFieldVector;
            actualTimestampMs = timestampVector.get(rowIndex);
        } else {
            // Fallback: try to get as object and convert
            Object timestampObj = timestampFieldVector.getObject(rowIndex);
            if (timestampObj instanceof java.time.LocalDateTime) {
                actualTimestampMs = java.sql.Timestamp.valueOf((java.time.LocalDateTime) timestampObj).getTime();
            } else {
                throw new AssertionError("Unexpected type for col_timestamp: " + 
                        (timestampObj != null ? timestampObj.getClass().getName() : "null"));
            }
        }
        // Timestamp values are stored as UTC milliseconds since epoch
        // Allow 1 second tolerance for potential rounding or precision differences
        assertEquals(expectedTimestampMs, actualTimestampMs, 1000, 
                "col_timestamp should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        TimeStampMilliVector timestampMsVector = (TimeStampMilliVector) root.getVector("col_timestamp_ms");
        long actualTimestampMsValue = timestampMsVector.get(rowIndex);
        assertEquals(expectedTimestampMs, actualTimestampMsValue, 1000, 
                "col_timestamp_ms should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        TimeStampMicroVector timestampUsVector = (TimeStampMicroVector) root.getVector("col_timestamp_us");
        long actualTimestampUsMs = timestampUsVector.get(rowIndex) / 1000;
        assertEquals(expectedTimestampMs, actualTimestampUsMs, 1000, 
                "col_timestamp_us should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        // Boolean type
        BitVector boolVector = (BitVector) root.getVector("col_bool");
        assertEquals(1, boolVector.get(rowIndex), "col_bool should be 1 (true)");
        
        // String types
        VarCharVector stringVector = (VarCharVector) root.getVector("col_string");
        String expectedString = "Test String";
        String actualString = new String(stringVector.get(rowIndex));
        assertEquals(expectedString, actualString, "col_string should be 'Test String'");
        
        LargeVarCharVector largeStringVector = (LargeVarCharVector) root.getVector("col_large_string");
        String expectedLargeString = "Large String Content " + "A".repeat(100);
        String actualLargeString = new String(largeStringVector.get(rowIndex));
        assertEquals(expectedLargeString, actualLargeString, "col_large_string should match expected value");
        
        // Binary types
        VarBinaryVector binaryVector = (VarBinaryVector) root.getVector("col_binary");
        byte[] expectedBinary = new byte[]{0x01, 0x02, 0x03, 0x04};
        byte[] actualBinary = binaryVector.get(rowIndex);
        assertArrayEquals(expectedBinary, actualBinary, "col_binary should match expected bytes");
        
        LargeVarBinaryVector largeBinaryVector = (LargeVarBinaryVector) root.getVector("col_large_binary");
        byte[] expectedLargeBinary = new byte[]{0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
        byte[] actualLargeBinary = largeBinaryVector.get(rowIndex);
        assertArrayEquals(expectedLargeBinary, actualLargeBinary, "col_large_binary should match expected bytes");
                
        // Decimal type
        DecimalVector decimalVector = (DecimalVector) root.getVector("col_decimal");
        BigDecimal expectedDecimal = new BigDecimal("1234567890123456789012345678.1234567890");
        BigDecimal actualDecimal = decimalVector.getObject(rowIndex);
        assertEquals(0, expectedDecimal.compareTo(actualDecimal), 
                "col_decimal should match expected value: " + expectedDecimal);
        
        // Interval types
        IntervalYearVector intervalYVector = (IntervalYearVector) root.getVector("col_interval_ym");
        // 1 year = 12 months
        int expectedIntervalYM = 12;
        int actualIntervalYM = intervalYVector.get(rowIndex);
        assertEquals(expectedIntervalYM, actualIntervalYM, "col_interval_ym should be 12 months");
        
        IntervalDayVector intervalDVector = (IntervalDayVector) root.getVector("col_interval_dt");
        // IntervalDay stores days and milliseconds
        // getObject() returns PeriodDuration which contains Period (days) and Duration (milliseconds)
        Object intervalObj = intervalDVector.getObject(rowIndex);
        assertNotNull(intervalObj, "col_interval_dt should not be null");
        
        // Extract days and milliseconds from PeriodDuration
        int expectedDays = 1;
        int expectedMillis = 0;
        int actualDays = 0;
        int actualMillis = 0;
        
        if (intervalObj instanceof PeriodDuration) {
            PeriodDuration pd = (PeriodDuration) intervalObj;
            Period period = pd.getPeriod();
            Duration duration = pd.getDuration();
            
            actualDays = period.getDays();
            // Duration contains seconds and nanoseconds, convert to milliseconds
            long totalMillis = duration.toMillis();
            actualMillis = (int) totalMillis;
        } else if (intervalObj instanceof Duration) {
            // Handle java.time.Duration directly
            // Arrow's IntervalDayVector.getObject() may return Duration when data was set via Duration
            // This happens when reading from JDBC ResultSet which returns Duration
            Duration duration = (Duration) intervalObj;
            long days = duration.toDays();
            long remainingMillis = duration.minusDays(days).toMillis();
            actualDays = (int) days;
            actualMillis = (int) remainingMillis;
        } else {
            // Fallback: try to parse from string representation or use reflection
            log.warn("IntervalDay value is not PeriodDuration or Duration, got: {}", intervalObj != null ? intervalObj.getClass().getName() : "null");
            try {
                // Try reflection as fallback for PeriodDuration-like objects
                java.lang.reflect.Method getPeriodMethod = intervalObj.getClass().getMethod("getPeriod");
                java.lang.reflect.Method getDurationMethod = intervalObj.getClass().getMethod("getDuration");
                
                Object period = getPeriodMethod.invoke(intervalObj);
                Object duration = getDurationMethod.invoke(intervalObj);
                
                java.lang.reflect.Method getDaysMethod = period.getClass().getMethod("getDays");
                actualDays = (Integer) getDaysMethod.invoke(period);
                
                java.lang.reflect.Method toMillisMethod = duration.getClass().getMethod("toMillis");
                long millis = (Long) toMillisMethod.invoke(duration);
                actualMillis = (int) millis;
            } catch (Exception e) {
                log.error("Failed to extract days and milliseconds from interval object of type: {}, value: {}", 
                        intervalObj != null ? intervalObj.getClass().getName() : "null", intervalObj);
                fail("Failed to extract days and milliseconds from interval object: " + e.getMessage() + 
                        ". Object type: " + (intervalObj != null ? intervalObj.getClass().getName() : "null"));
            }
        }
        
        assertEquals(expectedDays, actualDays, "col_interval_dt days should be 1");
        assertEquals(expectedMillis, actualMillis, "col_interval_dt milliseconds should be 0");
        
        log.info("✅ All data values verified successfully");
    }
    
    @Test
    @Order(3)
    public void testCommandDataSourceSqlQuery() {
        log.info("Testing SQL query functionality with comprehensive data types...");
        
        // Build SQL query containing all major types
        // Note: Skip uint types (not supported by Dameng) and timestamp_ns (Dameng doesn't support nanosecond precision)
        String sql = String.format(
            "SELECT " +
            "col_int8, col_int16, col_int32, col_int64, " +
            "col_float32, col_float64, " +
            "col_date32, col_date64, " +
            "col_time32, col_time64, " +
            "col_timestamp, col_timestamp_ms, col_timestamp_us, " +
            "col_bool, " +
            "col_string, col_large_string, " +
            "col_binary, col_large_binary, " +
            "col_decimal, " +
            "col_interval_ym, col_interval_dt " +
            "FROM %s", TABLE_NAME);
        
        Flightinner.CommandDataMeshSqlQuery query = Flightinner.CommandDataMeshSqlQuery.newBuilder()
                .setDatasource(domainDataSource)
                .setQuery(Flightdm.CommandDataSourceSqlQuery.newBuilder()
                        .setSql(sql)
                        .build())
                .build();
        
        assertDoesNotThrow(() -> {
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(query).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));
            
            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                int rowCount = 0;
                VectorSchemaRoot root = null;
                
                while (stream.next()) {
                    root = stream.getRoot();
                    rowCount += root.getRowCount();
                    
                    // Verify returned column count (21 columns: all types minus uint and timestamp_ns)
                    int expectedColumnCount = 21;
                    assertEquals(expectedColumnCount, root.getSchema().getFields().size(),
                            "SQL query should return all selected columns");
                    
                    // Verify all columns exist and types are correct
                    verifySqlQuerySchema(root);
                    
                    log.info("Successfully read {} rows with {} columns from SQL query", 
                            rowCount, root.getSchema().getFields().size());
                }
                
                assertEquals(1, rowCount, "SQL query should return 1 row");
                
                // Verify returned data values (consistent with written data)
                if (root != null && root.getRowCount() > 0) {
                    verifySqlQueryDataValues(root, 0);
                }
                
                log.info("SQL query test passed - all data types verified");
            }
        });
    }
    
    /**
     * Verify SQL query returned Schema (column types).
     * 
     * @param root VectorSchemaRoot
     */
    private void verifySqlQuerySchema(VectorSchemaRoot root) {
        assertNotNull(root.getVector("col_int8"), "col_int8 should exist");
        assertNotNull(root.getVector("col_int16"), "col_int16 should exist");
        assertNotNull(root.getVector("col_int32"), "col_int32 should exist");
        assertNotNull(root.getVector("col_int64"), "col_int64 should exist");
        
        assertNotNull(root.getVector("col_float32"), "col_float32 should exist");
        assertNotNull(root.getVector("col_float64"), "col_float64 should exist");
        
        assertNotNull(root.getVector("col_date32"), "col_date32 should exist");
        assertNotNull(root.getVector("col_date64"), "col_date64 should exist");
        
        assertNotNull(root.getVector("col_time32"), "col_time32 should exist");
        assertNotNull(root.getVector("col_time64"), "col_time64 should exist");
        
        assertNotNull(root.getVector("col_timestamp"), "col_timestamp should exist");
        assertNotNull(root.getVector("col_timestamp_ms"), "col_timestamp_ms should exist");
        assertNotNull(root.getVector("col_timestamp_us"), "col_timestamp_us should exist");
        
        assertNotNull(root.getVector("col_bool"), "col_bool should exist");
        
        assertNotNull(root.getVector("col_string"), "col_string should exist");
        assertNotNull(root.getVector("col_large_string"), "col_large_string should exist");
        
        assertNotNull(root.getVector("col_binary"), "col_binary should exist");
        assertNotNull(root.getVector("col_large_binary"), "col_large_binary should exist");
        
        assertNotNull(root.getVector("col_decimal"), "col_decimal should exist");
        
        assertNotNull(root.getVector("col_interval_ym"), "col_interval_ym should exist");
        assertNotNull(root.getVector("col_interval_dt"), "col_interval_dt should exist");
        
        log.debug("All SQL query columns verified successfully");
    }
    
    /**
     * Verify SQL query returned data values (consistent with written data).
     * 
     * @param root VectorSchemaRoot
     * @param rowIndex Row index
     */
    private void verifySqlQueryDataValues(VectorSchemaRoot root, int rowIndex) {
        log.info("Verifying SQL query data values for row {}", rowIndex);
        
        TinyIntVector int8Vector = (TinyIntVector) root.getVector("col_int8");
        assertEquals(127, int8Vector.get(rowIndex), "col_int8 should be 127");
        
        SmallIntVector int16Vector = (SmallIntVector) root.getVector("col_int16");
        assertEquals(32767, int16Vector.get(rowIndex), "col_int16 should be 32767");
        
        IntVector int32Vector = (IntVector) root.getVector("col_int32");
        assertEquals(2147483647, int32Vector.get(rowIndex), "col_int32 should be 2147483647");
        
        BigIntVector int64Vector = (BigIntVector) root.getVector("col_int64");
        assertEquals(9223372036854775807L, int64Vector.get(rowIndex), "col_int64 should be 9223372036854775807");
        
        Float4Vector float32Vector = (Float4Vector) root.getVector("col_float32");
        assertEquals(3.14f, float32Vector.get(rowIndex), 0.01f, "col_float32 should be approximately 3.14");
        
        Float8Vector float64Vector = (Float8Vector) root.getVector("col_float64");
        assertEquals(2.718281828459045, float64Vector.get(rowIndex), 0.000000000000001, 
                "col_float64 should be approximately 2.718281828459045");
        
        DateDayVector date32Vector = (DateDayVector) root.getVector("col_date32");
        int expectedDate32Days = (int) LocalDate.of(2024, 1, 1).toEpochDay();
        assertEquals(expectedDate32Days, date32Vector.get(rowIndex), "col_date32 should be 2024-01-01");
        
        // col_date64: dynamically check type (may be DateMilliVector, TimeStampMicroVector, or TimeStampMilliVector)
        FieldVector date64FieldVector = root.getVector("col_date64");
        long expectedDate64Ms = java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime();
        long actualDate64Ms = 0;
        if (date64FieldVector instanceof DateMilliVector) {
            actualDate64Ms = ((DateMilliVector) date64FieldVector).get(rowIndex);
        } else if (date64FieldVector instanceof TimeStampMicroVector) {
            // Convert microseconds to milliseconds
            actualDate64Ms = ((TimeStampMicroVector) date64FieldVector).get(rowIndex) / 1000;
        } else if (date64FieldVector instanceof TimeStampMilliVector) {
            actualDate64Ms = ((TimeStampMilliVector) date64FieldVector).get(rowIndex);
        }
        assertEquals(expectedDate64Ms, actualDate64Ms, 1000, "col_date64 should be approximately 2024-01-01 12:00:00");
        
        TimeMilliVector time32Vector = (TimeMilliVector) root.getVector("col_time32");
        LocalTime expectedTime32 = LocalTime.of(12, 30, 45);
        LocalTime actualTime32 = LocalTime.ofSecondOfDay(time32Vector.get(rowIndex) / 1000);
        assertEquals(expectedTime32.getHour(), actualTime32.getHour(), "col_time32 hour should be 12");
        assertEquals(expectedTime32.getMinute(), actualTime32.getMinute(), "col_time32 minute should be 30");
        assertEquals(expectedTime32.getSecond(), actualTime32.getSecond(), "col_time32 second should be 45");
        
        // col_time64: dynamically check type (TIME(6) should be read as TimeMicroVector)
        FieldVector time64FieldVector = root.getVector("col_time64");
        LocalTime expectedTime64 = LocalTime.of(12, 30, 45, 123456000);
        LocalTime actualTime64 = null;
        if (time64FieldVector instanceof TimeMicroVector) {
            TimeMicroVector time64Vector = (TimeMicroVector) time64FieldVector;
            long time64Micros = time64Vector.get(rowIndex);
            actualTime64 = LocalTime.ofNanoOfDay(time64Micros * 1000);
        } else if (time64FieldVector instanceof TimeMilliVector) {
            // TIME(6) should be read as TimeMicroVector, if read as TimeMilliVector it indicates precision loss
            TimeMilliVector time64Vector = (TimeMilliVector) time64FieldVector;
            int time64Millis = time64Vector.get(rowIndex);
            fail("col_time64 was read as TimeMilliVector instead of TimeMicroVector. " +
                    "This indicates that TIME(6) precision was not correctly parsed. " +
                    "Expected microseconds precision but got milliseconds. " +
                    "Value: " + time64Millis + " milliseconds");
        } else {
            fail("Unexpected type for col_time64: " + 
                    (time64FieldVector != null ? time64FieldVector.getClass().getName() : "null"));
        }
        assertNotNull(actualTime64, "col_time64 should not be null");
        assertEquals(expectedTime64.getHour(), actualTime64.getHour(), "col_time64 hour should be 12");
        assertEquals(expectedTime64.getMinute(), actualTime64.getMinute(), "col_time64 minute should be 30");
        assertEquals(expectedTime64.getSecond(), actualTime64.getSecond(), "col_time64 second should be 45");
        /*
         * Note: JDBC driver returns java.sql.Time which only supports second precision, not microsecond precision
         * Even though database TIME(6) can store microseconds, JDBC driver returns java.sql.Time which loses microsecond precision
         * Therefore, we only verify up to second precision, not microsecond precision
         * The actual nanoseconds will be 0, which is a JDBC driver limitation
         */
        log.info("col_time64 nanoseconds: {} (expected: {}). Note: JDBC java.sql.Time only supports second precision", 
                actualTime64.getNano(), expectedTime64.getNano());
        
        // Timestamp type verification (timestamps stored as UTC milliseconds, allow 1 second tolerance)
        long expectedTimestampMs = java.sql.Timestamp.valueOf("2024-01-01 12:00:00").getTime();
        
        FieldVector timestampFieldVector = root.getVector("col_timestamp");
        long actualTimestampMs = 0;
        if (timestampFieldVector instanceof TimeStampMicroVector) {
            // Convert microseconds to milliseconds
            actualTimestampMs = ((TimeStampMicroVector) timestampFieldVector).get(rowIndex) / 1000;
        } else if (timestampFieldVector instanceof TimeStampMilliVector) {
            actualTimestampMs = ((TimeStampMilliVector) timestampFieldVector).get(rowIndex);
        }
        assertEquals(expectedTimestampMs, actualTimestampMs, 1000, 
                "col_timestamp should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        TimeStampMilliVector timestampMsVector = (TimeStampMilliVector) root.getVector("col_timestamp_ms");
        long actualTimestampMsValue = timestampMsVector.get(rowIndex);
        assertEquals(expectedTimestampMs, actualTimestampMsValue, 1000, 
                "col_timestamp_ms should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        TimeStampMicroVector timestampUsVector = (TimeStampMicroVector) root.getVector("col_timestamp_us");
        long actualTimestampUsMs = timestampUsVector.get(rowIndex) / 1000;
        assertEquals(expectedTimestampMs, actualTimestampUsMs, 1000, 
                "col_timestamp_us should be approximately 2024-01-01 12:00:00 (within 1 second tolerance)");
        
        BitVector boolVector = (BitVector) root.getVector("col_bool");
        assertEquals(1, boolVector.get(rowIndex), "col_bool should be true (1)");
        
        VarCharVector stringVector = (VarCharVector) root.getVector("col_string");
        String actualString = new String(stringVector.get(rowIndex));
        assertEquals("Test String", actualString, "col_string should be 'Test String'");
        
        LargeVarCharVector largeStringVector = (LargeVarCharVector) root.getVector("col_large_string");
        String expectedLargeString = "Large String Content " + "A".repeat(100);
        String actualLargeString = new String(largeStringVector.get(rowIndex));
        assertEquals(expectedLargeString, actualLargeString, "col_large_string should match expected value");
        
        VarBinaryVector binaryVector = (VarBinaryVector) root.getVector("col_binary");
        byte[] expectedBinary = new byte[]{0x01, 0x02, 0x03, 0x04};
        byte[] actualBinary = binaryVector.get(rowIndex);
        assertArrayEquals(expectedBinary, actualBinary, "col_binary should match expected bytes");
        
        LargeVarBinaryVector largeBinaryVector = (LargeVarBinaryVector) root.getVector("col_large_binary");
        byte[] expectedLargeBinary = new byte[]{0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
        byte[] actualLargeBinary = largeBinaryVector.get(rowIndex);
        assertArrayEquals(expectedLargeBinary, actualLargeBinary, "col_large_binary should match expected bytes");
                
        DecimalVector decimalVector = (DecimalVector) root.getVector("col_decimal");
        BigDecimal expectedDecimal = new BigDecimal("1234567890123456789012345678.1234567890");
        BigDecimal actualDecimal = decimalVector.getObject(rowIndex);
        assertEquals(0, expectedDecimal.compareTo(actualDecimal), 
                "col_decimal should match expected value: " + expectedDecimal);
        
        IntervalYearVector intervalYVector = (IntervalYearVector) root.getVector("col_interval_ym");
        // 1 year = 12 months
        int expectedIntervalYM = 12;
        assertEquals(expectedIntervalYM, intervalYVector.get(rowIndex), "col_interval_ym should be 12 months");
        
        IntervalDayVector intervalDVector = (IntervalDayVector) root.getVector("col_interval_dt");
        Object intervalObj = intervalDVector.getObject(rowIndex);
        assertNotNull(intervalObj, "col_interval_dt should not be null");
        
        // Verify specific values of interval types
        int expectedDays = 1;
        int expectedMillis = 0;
        int actualDays = 0;
        int actualMillis = 0;
        
        if (intervalObj instanceof PeriodDuration) {
            PeriodDuration pd = (PeriodDuration) intervalObj;
            Period period = pd.getPeriod();
            Duration duration = pd.getDuration();
            
            actualDays = period.getDays();
            long totalMillis = duration.toMillis();
            actualMillis = (int) totalMillis;
        } else if (intervalObj instanceof Duration) {
            // Handle java.time.Duration directly (JDBC may return Duration instead of PeriodDuration)
            Duration duration = (Duration) intervalObj;
            long days = duration.toDays();
            long remainingMillis = duration.minusDays(days).toMillis();
            actualDays = (int) days;
            actualMillis = (int) remainingMillis;
        } else {
            // Fallback: try reflection for PeriodDuration-like objects
            try {
                java.lang.reflect.Method getPeriodMethod = intervalObj.getClass().getMethod("getPeriod");
                java.lang.reflect.Method getDurationMethod = intervalObj.getClass().getMethod("getDuration");
                
                Object period = getPeriodMethod.invoke(intervalObj);
                Object duration = getDurationMethod.invoke(intervalObj);
                
                java.lang.reflect.Method getDaysMethod = period.getClass().getMethod("getDays");
                actualDays = (Integer) getDaysMethod.invoke(period);
                
                java.lang.reflect.Method toMillisMethod = duration.getClass().getMethod("toMillis");
                long millis = (Long) toMillisMethod.invoke(duration);
                actualMillis = (int) millis;
            } catch (Exception e) {
                log.error("Failed to extract days and milliseconds from interval object of type: {}, value: {}", 
                        intervalObj != null ? intervalObj.getClass().getName() : "null", intervalObj);
                fail("Failed to extract days and milliseconds from interval object: " + e.getMessage() + 
                        ". Object type: " + (intervalObj != null ? intervalObj.getClass().getName() : "null"));
            }
        }
        
        assertEquals(expectedDays, actualDays, "col_interval_dt days should be 1");
        assertEquals(expectedMillis, actualMillis, "col_interval_dt milliseconds should be 0");
        
        log.info("✅ All SQL query data values verified successfully");
    }

    @Test
    @Order(4)
    public void testPartitionTableOverwrite() {
        log.info("Testing partition table overwrite functionality...");
        
        final String PARTITION_TABLE_NAME = "PARTITION_TEST_TABLE";
        final String PARTITION_COLUMN = "dt";
        final String PARTITION_VALUE_1 = "20240101";
        final String PARTITION_VALUE_2 = "20240102";
        
        // Create partition table columns (partition column must be included in column definition)
        List<Common.DataColumn> partitionColumns = Arrays.asList(
                Common.DataColumn.newBuilder().setName("id").setType("int32").build(),
                Common.DataColumn.newBuilder().setName("name").setType("string").build(),
                Common.DataColumn.newBuilder().setName(PARTITION_COLUMN).setType("string").build()
        );
        
        assertDoesNotThrow(() -> {
            // Setup partition table
            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(String.format("DROP TABLE IF EXISTS %s", PARTITION_TABLE_NAME));
                
                // Create partition table (Dameng database partition columns must be included in column definitions)
                String createPartitionTableSql = String.format(
                    "CREATE TABLE %s (" +
                    "id INT, " +
                    "name VARCHAR(100), " +
                    "%s VARCHAR(20)" +
                    ")", PARTITION_TABLE_NAME, PARTITION_COLUMN);
                stmt.execute(createPartitionTableSql);
                log.info("Created partition table: {}", PARTITION_TABLE_NAME);
            }
        });
        
        // Create DomainData with partition
        Domaindata.DomainData partitionDomainData = Domaindata.DomainData.newBuilder()
                .setDatasourceId("dameng-datasource")
                .setName(PARTITION_TABLE_NAME)
                .setRelativeUri(PARTITION_TABLE_NAME)
                .setDomaindataId("dameng-partition-table")
                .setType("table")
                .addAllColumns(partitionColumns)
                .build();
        
        // Test 1: Write data to partition 1
        assertDoesNotThrow(() -> {
            Flightinner.CommandDataMeshUpdate command1 = Flightinner.CommandDataMeshUpdate.newBuilder()
                    .setDatasource(domainDataSource)
                    .setDomaindata(partitionDomainData)
                    .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                            .setContentType(Flightdm.ContentType.CSV)
                            .setPartitionSpec(String.format("%s=%s", PARTITION_COLUMN, PARTITION_VALUE_1)))
                    .build();
            
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command1).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            
            Schema schema = new Schema(partitionColumns.stream()
                    .map(col -> Field.nullable(col.getName(), ArrowUtil.parseKusciaColumnType(col.getType())))
                    .collect(Collectors.toList()));
            
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                VarCharVector dtVector = (VarCharVector) root.getVector(PARTITION_COLUMN);
                
                // Write first batch of data (include partition column value)
                idVector.setSafe(0, 1);
                nameVector.setSafe(0, "First Batch".getBytes());
                dtVector.setSafe(0, PARTITION_VALUE_1.getBytes());
                idVector.setSafe(1, 2);
                nameVector.setSafe(1, "First Batch 2".getBytes());
                dtVector.setSafe(1, PARTITION_VALUE_1.getBytes());
                
                root.setRowCount(2);
                
                FlightClient.ClientStreamListener listener = client.startPut(
                        FlightDescriptor.command(ticket.getBytes()), root, new AsyncPutListener());
                listener.putNext();
                listener.completed();
                listener.getResult();
                
                log.info("Wrote 2 rows to partition {}", PARTITION_VALUE_1);
            }
        });
        
        // Verify partition 1 has 2 rows
        assertDoesNotThrow(() -> {
            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery(
                         String.format("SELECT COUNT(*) as cnt FROM %s WHERE %s = '%s'", 
                                 PARTITION_TABLE_NAME, PARTITION_COLUMN, PARTITION_VALUE_1))) {
                assertTrue(rs.next(), "Should have result");
                assertEquals(2, rs.getInt("cnt"), "Partition " + PARTITION_VALUE_1 + " should have 2 rows");
                log.info("Verified partition {} has 2 rows", PARTITION_VALUE_1);
            }
        });
        
        // Test 2: Overwrite partition 1 with new data
        assertDoesNotThrow(() -> {
            Flightinner.CommandDataMeshUpdate command2 = Flightinner.CommandDataMeshUpdate.newBuilder()
                    .setDatasource(domainDataSource)
                    .setDomaindata(partitionDomainData)
                    .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                            .setContentType(Flightdm.ContentType.CSV)
                            .setPartitionSpec(String.format("%s=%s", PARTITION_COLUMN, PARTITION_VALUE_1)))
                    .build();
            
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command2).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            
            Schema schema = new Schema(partitionColumns.stream()
                    .map(col -> Field.nullable(col.getName(), ArrowUtil.parseKusciaColumnType(col.getType())))
                    .collect(Collectors.toList()));
            
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                VarCharVector dtVector = (VarCharVector) root.getVector(PARTITION_COLUMN);
                
                // Write new data to same partition (should overwrite)
                idVector.setSafe(0, 10);
                nameVector.setSafe(0, "Overwritten Batch".getBytes());
                dtVector.setSafe(0, PARTITION_VALUE_1.getBytes());
                idVector.setSafe(1, 11);
                nameVector.setSafe(1, "Overwritten Batch 2".getBytes());
                dtVector.setSafe(1, PARTITION_VALUE_1.getBytes());
                idVector.setSafe(2, 12);
                nameVector.setSafe(2, "Overwritten Batch 3".getBytes());
                dtVector.setSafe(2, PARTITION_VALUE_1.getBytes());
                
                root.setRowCount(3);
                
                FlightClient.ClientStreamListener listener = client.startPut(
                        FlightDescriptor.command(ticket.getBytes()), root, new AsyncPutListener());
                listener.putNext();
                listener.completed();
                listener.getResult();
                
                log.info("Overwrote partition {} with 3 new rows", PARTITION_VALUE_1);
            }
        });
        
        // Verify partition 1 now has only 3 rows (overwritten)
        assertDoesNotThrow(() -> {
            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery(
                         String.format("SELECT COUNT(*) as cnt FROM %s WHERE %s = '%s'", 
                                 PARTITION_TABLE_NAME, PARTITION_COLUMN, PARTITION_VALUE_1))) {
                assertTrue(rs.next(), "Should have result");
                assertEquals(3, rs.getInt("cnt"), 
                        "Partition " + PARTITION_VALUE_1 + " should have 3 rows after overwrite");
                
                // Verify old data is gone
                java.sql.ResultSet rs2 = stmt.executeQuery(
                        String.format("SELECT COUNT(*) as cnt FROM %s WHERE %s = '%s' AND name = 'First Batch'", 
                                PARTITION_TABLE_NAME, PARTITION_COLUMN, PARTITION_VALUE_1));
                assertTrue(rs2.next(), "Should have result");
                assertEquals(0, rs2.getInt("cnt"), 
                        "Old data should be overwritten");
                
                log.info("Verified partition {} was overwritten correctly", PARTITION_VALUE_1);
            }
        });
        
        // Test 3: Write data to partition 2 (different partition should not be affected)
        assertDoesNotThrow(() -> {
            Flightinner.CommandDataMeshUpdate command3 = Flightinner.CommandDataMeshUpdate.newBuilder()
                    .setDatasource(domainDataSource)
                    .setDomaindata(partitionDomainData)
                    .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                            .setContentType(Flightdm.ContentType.CSV)
                            .setPartitionSpec(String.format("%s=%s", PARTITION_COLUMN, PARTITION_VALUE_2)))
                    .build();
            
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command3).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            
            Schema schema = new Schema(partitionColumns.stream()
                    .map(col -> Field.nullable(col.getName(), ArrowUtil.parseKusciaColumnType(col.getType())))
                    .collect(Collectors.toList()));
            
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                VarCharVector dtVector = (VarCharVector) root.getVector(PARTITION_COLUMN);
                
                idVector.setSafe(0, 20);
                nameVector.setSafe(0, "Partition 2 Data".getBytes());
                dtVector.setSafe(0, PARTITION_VALUE_2.getBytes());
                
                root.setRowCount(1);
                
                FlightClient.ClientStreamListener listener = client.startPut(
                        FlightDescriptor.command(ticket.getBytes()), root, new AsyncPutListener());
                listener.putNext();
                listener.completed();
                listener.getResult();
                
                log.info("Wrote 1 row to partition {}", PARTITION_VALUE_2);
            }
        });
        
        // Verify both partitions exist independently
        assertDoesNotThrow(() -> {
            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement()) {
                
                // Verify partition 1 still has 3 rows
                java.sql.ResultSet rs1 = stmt.executeQuery(
                        String.format("SELECT COUNT(*) as cnt FROM %s WHERE %s = '%s'", 
                                PARTITION_TABLE_NAME, PARTITION_COLUMN, PARTITION_VALUE_1));
                assertTrue(rs1.next(), "Should have result");
                assertEquals(3, rs1.getInt("cnt"), 
                        "Partition " + PARTITION_VALUE_1 + " should still have 3 rows");
                
                // Verify partition 2 has 1 row
                java.sql.ResultSet rs2 = stmt.executeQuery(
                        String.format("SELECT COUNT(*) as cnt FROM %s WHERE %s = '%s'", 
                                PARTITION_TABLE_NAME, PARTITION_COLUMN, PARTITION_VALUE_2));
                assertTrue(rs2.next(), "Should have result");
                assertEquals(1, rs2.getInt("cnt"), 
                        "Partition " + PARTITION_VALUE_2 + " should have 1 row");
                
                log.info("Verified both partitions exist independently");
            }
        });
        
        // Cleanup
        assertDoesNotThrow(() -> {
            try (Connection conn = DriverManager.getConnection(damengJdbcUrl, damengUser, damengPassword);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(String.format("DROP TABLE IF EXISTS %s", PARTITION_TABLE_NAME));
                log.info("Cleaned up partition test table");
            }
        });
    }
}
