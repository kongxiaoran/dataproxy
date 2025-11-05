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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.utils.Record;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test class for DatabaseRecordSender.
 * 
 * @author kongxiaoran
 * @date 2025/11/07
 */
@ExtendWith(MockitoExtension.class)
public class DatabaseRecordSenderTest {

    @Mock
    private DatabaseMetaData mockMetaData;

    @Mock
    private ResultSet mockResultSet;

    private RootAllocator allocator;
    private VectorSchemaRoot root;
    private Schema schema;
    private LinkedBlockingQueue<Record> recordQueue;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator();
        schema = new Schema(java.util.Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("price", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
                new Field("is_active", FieldType.nullable(ArrowType.Bool.INSTANCE), null)
        ));
        
        root = VectorSchemaRoot.create(schema, allocator);
        recordQueue = new LinkedBlockingQueue<>(1000);
    }

    @Test
    public void testConstructor() {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        assertNotNull(sender);
    }

    @Test
    public void testToArrowVector_WithIntAndString() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("id", 1);
        record.set("name", "test");
        
        sender.toArrowVector(record, root, 0);
        
        // Verify data has been set
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        
        assertEquals(1, idVector.get(0));
        // VarCharVector.get() returns byte[], need to convert to String
        assertEquals("test", new String(nameVector.get(0)));
    }

    @Test
    public void testToArrowVector_WithNullValue() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("id", null);
        record.set("name", "test");
        
        sender.toArrowVector(record, root, 0);
        
        // Verify null value has been correctly set
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        
        assertTrue(idVector.isNull(0));
        // VarCharVector.get() returns byte[], need to convert to String
        assertEquals("test", new String(nameVector.get(0)));
    }

    @Test
    public void testToArrowVector_WithMissingColumn() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("id", 1);
        // name column missing, should be ignored
        
        assertDoesNotThrow(() -> {
            sender.toArrowVector(record, root, 0);
        });
        
        IntVector idVector = (IntVector) root.getVector("id");
        assertEquals(1, idVector.get(0));
    }

    @Test
    public void testToArrowVector_WithBoolean() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("is_active", true);
        
        sender.toArrowVector(record, root, 0);
        
        BitVector boolVector = (BitVector) root.getVector("is_active");
        assertEquals(1, boolVector.get(0)); // true = 1
    }

    @Test
    public void testToArrowVector_WithDecimal() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        java.math.BigDecimal expectedValue = java.math.BigDecimal.valueOf(99.99);
        record.set("price", expectedValue);
        
        sender.toArrowVector(record, root, 0);
        
        DecimalVector decimalVector = (DecimalVector) root.getVector("price");
        assertNotNull(decimalVector);
        assertFalse(decimalVector.isNull(0));
        
        // Verify the decimal value is correctly set
        java.math.BigDecimal actualValue = decimalVector.getObject(0);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testIsOver_WithLastLine() {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.setLast(true);
        
        assertTrue(sender.isOver(record));
    }

    @Test
    public void testIsOver_WithoutLastLine() {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.setLast(false);
        
        assertFalse(sender.isOver(record));
    }

    @Test
    public void testPutOver() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        sender.putOver();
        
        // Poll record from queue and verify
        Record lastRecord = recordQueue.poll();
        assertNotNull(lastRecord, "Queue should contain a record after putOver()");
        assertTrue(lastRecord.isLastLine(), "Last record should have isLast flag set to true");
        assertNotNull(lastRecord.getData());
        assertTrue(lastRecord.getData().isEmpty(), "Last record should have no data");
    }

    @Test
    public void testEqualsIgnoreCase() {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        assertTrue(sender.equalsIgnoreCase("TEST", "test"));
        assertTrue(sender.equalsIgnoreCase("test", "TEST"));
        assertTrue(sender.equalsIgnoreCase("Test", "test"));
        assertTrue(sender.equalsIgnoreCase(null, null));
        assertFalse(sender.equalsIgnoreCase("test", "other"));
        assertFalse(sender.equalsIgnoreCase(null, "test"));
        assertFalse(sender.equalsIgnoreCase("test", null));
    }

    @Test
    public void testInitRecordColumn2FieldMap() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("id", 1);
        record.set("name", "test");
        
        // First call will initialize mapping
        sender.toArrowVector(record, root, 0);
        
        // Second call should use already initialized mapping
        Record record2 = new Record();
        record2.set("id", 2);
        record2.set("name", "test2");
        
        assertDoesNotThrow(() -> {
            sender.toArrowVector(record2, root, 1);
        });
    }

    @Test
    public void testInitRecordColumn2FieldMap_WithNullRoot() {
        // AbstractSender constructor calls preAllocate(), if root is null will throw NPE
        // This is expected behavior, null root should not be allowed
        assertThrows(NullPointerException.class, () -> {
            new DatabaseRecordSender(
                    100,
                    recordQueue,
                    null,
                    "test_table",
                    mockMetaData,
                    mockResultSet
            );
        });
    }

    @Test
    public void testToArrowVector_WithUnsupportedType() throws InterruptedException {
        // Create a schema containing unsupported type
        Schema unsupportedSchema = new Schema(java.util.Arrays.asList(
                new Field("unknown", FieldType.nullable(ArrowType.Null.INSTANCE), null)
        ));
        
        VectorSchemaRoot unsupportedRoot = VectorSchemaRoot.create(unsupportedSchema, allocator);
        LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<>(100);
        
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                queue,
                unsupportedRoot,
                "test_table",
                mockMetaData,
                mockResultSet
        );
        
        Record record = new Record();
        record.set("unknown", "value");
        
        // Should not throw exception, but will log warning
        assertDoesNotThrow(() -> {
            sender.toArrowVector(record, unsupportedRoot, 0);
        });
    }

    @Test
    public void testToArrowVector_MultipleRows() throws InterruptedException {
        DatabaseRecordSender sender = new DatabaseRecordSender(
                100,
                recordQueue,
                root,
                "test_table",
                mockMetaData,
                mockResultSet
        );

        // Set multiple rows of data
        for (int i = 0; i < 3; i++) {
            Record record = new Record();
            record.set("id", i);
            record.set("name", "test" + i);
            
            sender.toArrowVector(record, root, i);
        }

        // Need to set rowCount to correctly read data
        root.setRowCount(3);
        
        // Verify all rows have been set
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        
        assertEquals(3, root.getRowCount());
        assertEquals(0, idVector.get(0));
        assertEquals(1, idVector.get(1));
        assertEquals(2, idVector.get(2));
    }
}

