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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit test class for DatabaseReader.
 * 
 * @author kongxiaoran
 * @date 2025/11/07
 */
@ExtendWith(MockitoExtension.class)
public class DatabaseReaderTest {

    @Mock
    private DatabaseDoGetContext mockContext;

    @Mock
    private DatabaseDoGetTaskContext mockTaskContext;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private DatabaseMetaData mockDatabaseMetaData;

    private BufferAllocator allocator;
    private TaskConfig taskConfig;
    private Schema testSchema;
    private List<DatabaseReader> readersToCleanup = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator();
        testSchema = new Schema(java.util.Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        
        // Use lenient() to avoid UnnecessaryStubbingException
        lenient().when(mockContext.getSchema()).thenReturn(testSchema);
        lenient().when(mockContext.getResultSet()).thenReturn(mockResultSet);
        lenient().when(mockContext.getDatabaseMetaData()).thenReturn(mockDatabaseMetaData);
        lenient().when(mockContext.getTableName()).thenReturn("test_table");
        taskConfig = new TaskConfig(mockContext, 0);
        readersToCleanup.clear();
    }

    @AfterEach
    public void tearDown() {
        // Clean up all created DatabaseReader instances, ensure background threads are properly closed
        for (DatabaseReader reader : readersToCleanup) {
            try {
                reader.closeReadSource();
                // Close the reader completely to release VectorSchemaRoot
                reader.close();
            } catch (Exception e) {
                // Ignore exceptions during cleanup
            }
        }
        readersToCleanup.clear();
        
        // Close allocator - this will detect any memory leaks
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    public void testConstructor() {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        assertNotNull(reader);
    }

    @Test
    public void testReadSchema() {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        Schema schema = reader.readSchema();
        assertNotNull(schema);
        assertEquals(testSchema, schema);
    }

    @Test
    public void testBytesRead() {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        assertEquals(0, reader.bytesRead());
    }

    @Test
    public void testLoadNextBatch_WithError() {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        RuntimeException error = new RuntimeException("Test error");
        taskConfig.setError(error);

        assertThrows(RuntimeException.class, () -> {
            reader.loadNextBatch();
        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testLoadNextBatch_NoError() throws Exception {
        // Use mock DatabaseDoGetTaskContext to avoid starting real background thread
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        taskConfig.setError(null);
        
        // Use reflection to set mock dbDoGetTaskContext
        java.lang.reflect.Field field = DatabaseReader.class.getDeclaredField("dbDoGetTaskContext");
        field.setAccessible(true);
        field.set(reader, mockTaskContext);
        
        // Mock hasNext returns false, indicating no more data
        when(mockTaskContext.hasNext()).thenReturn(false);
        
        boolean result = reader.loadNextBatch();
        assertFalse(result);
    }

    @Test
    public void testCloseReadSource_WithTaskContext() throws Exception {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        // Use reflection to set dbDoGetTaskContext
        java.lang.reflect.Field field = DatabaseReader.class.getDeclaredField("dbDoGetTaskContext");
        field.setAccessible(true);
        field.set(reader, mockTaskContext);
        
        doNothing().when(mockTaskContext).close();
        
        reader.closeReadSource();
        
        verify(mockTaskContext, times(1)).close();
    }

    @Test
    public void testCloseReadSource_WithoutTaskContext() {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        assertDoesNotThrow(() -> {
            reader.closeReadSource();
        });
    }

    @Test
    public void testCloseReadSource_WithInterruptedException() throws Exception {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        java.lang.reflect.Field field = DatabaseReader.class.getDeclaredField("dbDoGetTaskContext");
        field.setAccessible(true);
        field.set(reader, mockTaskContext);
        
        doThrow(new InterruptedException("Test interrupt")).when(mockTaskContext).close();
        
        reader.closeReadSource();
        
        // Verify thread interrupt status is set
        assertTrue(Thread.currentThread().isInterrupted());
        // Clear interrupt status to avoid affecting subsequent tests
        Thread.interrupted();
    }

    @Test
    public void testCloseReadSource_WithException() throws Exception {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        java.lang.reflect.Field field = DatabaseReader.class.getDeclaredField("dbDoGetTaskContext");
        field.setAccessible(true);
        field.set(reader, mockTaskContext);
        
        Exception testException = new RuntimeException("Test exception");
        doThrow(testException).when(mockTaskContext).close();
        
        assertThrows(RuntimeException.class, () -> {
            reader.closeReadSource();
        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testPrepare() throws Exception {
        DatabaseReader reader = new DatabaseReader(allocator, taskConfig);
        readersToCleanup.add(reader);
        
        // Verify initial state is null
        java.lang.reflect.Field field = DatabaseReader.class.getDeclaredField("dbDoGetTaskContext");
        field.setAccessible(true);
        assertNull(field.get(reader), "dbDoGetTaskContext should be null initially");
        
        // Mock ResultSet behavior for DatabaseRecordReader
        when(mockResultSet.next()).thenReturn(false); // No rows, will immediately finish
        
        // Call prepare() - it should create and start DatabaseDoGetTaskContext
        // Even though it starts a background thread, we can verify the object was created
        reader.prepare();
        
        // Verify DatabaseDoGetTaskContext was created
        Object dbDoGetTaskContext = field.get(reader);
        assertNotNull(dbDoGetTaskContext, "dbDoGetTaskContext should be created after prepare()");
        assertTrue(dbDoGetTaskContext instanceof DatabaseDoGetTaskContext, 
                "dbDoGetTaskContext should be an instance of DatabaseDoGetTaskContext");
        
        // Wait a bit for the background thread to finish (since ResultSet.next() returns false)
        Thread.sleep(100);
        
        // Verify the background thread has started (hasNext should eventually become false)
        DatabaseDoGetTaskContext context = (DatabaseDoGetTaskContext) dbDoGetTaskContext;
        // The hasNext flag should eventually become false after the reader finishes
        // We wait a bit more to ensure the thread has processed
        int attempts = 0;
        while (context.hasNext() && attempts < 50) {
            Thread.sleep(50);
            attempts++;
        }
        // Context is managed by reader, no need to close separately
        
        // Clean up: close the reader to stop background thread and release resources
        reader.closeReadSource();
        
        // Wait for executor service to shutdown completely
        // DatabaseDoGetTaskContext.close() calls executorService.shutdown() but doesn't wait
        // We need to wait a bit to ensure the background thread has finished and released resources
        Thread.sleep(500);
        
        // Close the reader completely to release VectorSchemaRoot
        // This is necessary because VectorSchemaRoot is managed by ArrowReader
        reader.close();
        
        // Remove from cleanup list since we've already closed it
        readersToCleanup.remove(reader);
    }
}

