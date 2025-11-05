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

package org.secretflow.dataproxy.plugin.database.utils;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;

@Setter
@Slf4j
public class Record {
    // isLast means that this record is the last record
    private boolean isLast = false;
    private Vector<Object> values;
    private Map<String, Integer> columnNames;
    private Map<Integer, Integer> columnTypes;

    public Record() {
        values = new Vector<>();
        columnTypes = new HashMap<>();
        columnNames = new HashMap<>();
    }

    public Record(ResultSet resultSet) throws SQLException {
        values = new Vector<>();
        columnTypes = new HashMap<>();
        columnNames = new HashMap<>();
        this.fromResultSet(resultSet);
    }

    public Object get(String columnName) {
        return values.get(columnNames.get(columnName));
    }

    public void set(String columnName, Object value) {
        columnNames.putIfAbsent(columnName, columnNames.size());
        values.add(columnNames.get(columnName), value);
    }

    private void setColumnType(int index, int columnType) {
        columnTypes.put(index, columnType);
    }

    private void fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int columnType = metaData.getColumnType(i);
            String columnTypeName = metaData.getColumnTypeName(i);


            Object columnValue;
            try {
                columnValue = switch (columnType) {
                    // CLOB type - use stream reading for better performance and support for large CLOB
                    case Types.CLOB, Types.NCLOB -> {
                        Clob clob = rs.getClob(i);
                        if (clob == null) {
                            yield null;
                        }
                        try {
                            // Check CLOB size, throw exception if exceeds String maximum length
                            long clobLength = clob.length();
                            if (clobLength > Integer.MAX_VALUE) {
                                clob.free();
                                throw new SQLException(String.format(
                                        "CLOB size %d exceeds String maximum length (%d) for column %s. " +
                                        "Cannot read CLOB larger than 2GB into a String.",
                                        clobLength, Integer.MAX_VALUE, columnName));
                            }
                            // Use stream reading to avoid loading entire content into memory
                            try (Reader reader = clob.getCharacterStream()) {
                                String clobContent = readCharacterStreamAsString(reader);
                                clob.free();
                                yield clobContent;
                            }
                        } catch (SQLException e) {
                            throw e;
                        } catch (Exception e) {
                            log.error("Failed to read CLOB stream for column {}, will try getString(): {}", columnName, e.getMessage());
                            clob.free();
                            yield rs.getString(i);
                        }
                    }

                    // LONGVARBINARY type - large binary data, try getBytes() first, fallback to stream reading
                    case Types.LONGVARBINARY -> {
                        try {
                            byte[] bytes = rs.getBytes(i);
                            if (bytes != null) {
                                yield bytes;
                            }
                            // If getBytes() returns null, try stream reading
                            InputStream inputStream = rs.getBinaryStream(i);
                            if (inputStream == null) {
                                yield null;
                            }
                            try (inputStream) {
                                yield readBinaryStreamAsBytes(inputStream);
                            }
                        } catch (Exception e) {
                            log.error("Failed to read LONGVARBINARY for column {}: {}", columnName, e.getMessage());
                            yield null;
                        }
                    }

                    // BLOB type - use stream reading for better performance and support for large BLOB
                    case Types.BLOB -> {
                        Blob blob = rs.getBlob(i);
                        if (blob == null) {
                            yield null;
                        }
                        try {
                            // Check BLOB size, throw exception if exceeds byte[] maximum length
                            long blobLength = blob.length();
                            if (blobLength > Integer.MAX_VALUE) {
                                blob.free();
                                throw new SQLException(String.format(
                                        "BLOB size %d exceeds byte array maximum length (%d) for column %s. " +
                                        "Cannot read BLOB larger than 2GB into a byte array.",
                                        blobLength, Integer.MAX_VALUE, columnName));
                            }
                            // Use stream reading to avoid loading entire content into memory
                            try (InputStream inputStream = blob.getBinaryStream()) {
                                byte[] blobContent = readBinaryStreamAsBytes(inputStream);
                                blob.free();
                                yield blobContent;
                            }
                        } catch (SQLException e) {
                            throw e;
                        } catch (Exception e) {
                            log.error("Failed to read BLOB stream for column {}, will try getBytes(): {}", columnName, e.getMessage());
                            blob.free();
                            yield rs.getBytes(i);
                        }
                    }

                    // LONGVARCHAR type - may need stream reading
                    case Types.LONGVARCHAR, Types.LONGNVARCHAR -> {
                        String strValue = rs.getString(i);
                        if (strValue != null) {
                            yield strValue;
                        }

                        // If getString() returns null, try stream reading
                        Reader reader = rs.getCharacterStream(i);
                        if (reader == null) {
                            yield null;
                        }
                        try (reader) {
                            yield readCharacterStreamAsString(reader);
                        } catch (Exception e) {
                            log.error("Failed to read LONGVARCHAR stream for column {}: {}", columnName, e.getMessage());
                            yield null;
                        }
                    }

                    default -> rs.getObject(i);
                };
            } catch (SQLException  e) {
                log.error("Failed to read column {} (type: {}, typeName: {}): {}",
                        columnName, columnType, columnTypeName, e.getMessage());
                columnValue = rs.getObject(i);
            }

            this.set(columnName, columnValue);
            this.setColumnType(i, columnType);
        }
    }

    public boolean isLastLine() {
        return this.isLast;
    }

    public Map<String, Object> getData() {
        Map<String, Object> temp = new HashMap<>();
        for(Map.Entry<String, Integer> entry : this.columnNames.entrySet()) {
            temp.put(entry.getKey(), this.values.get(entry.getValue()));
        }
        return temp;
    }

    /**
     * Read content from character stream and convert to string.
     * 
     * Note: This method is limited by String maximum length (Integer.MAX_VALUE, approximately 2GB).
     * If character stream content exceeds this limit, may throw OutOfMemoryError or String length exception.
     * 
     * @param reader Character stream reader
     * @return Read string content
     * @throws IOException Thrown when reading fails
     */
    private String readCharacterStreamAsString(Reader reader) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            char[] buffer = new char[8192];
            int length;
            while ((length = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, length);
            }
            return writer.toString();
        }
    }

    /**
     * Read content from byte stream and convert to byte array.
     * 
     * Note: This method is limited by byte[] maximum length (Integer.MAX_VALUE, approximately 2GB).
     * If byte stream content exceeds this limit, may throw OutOfMemoryError or array length exception.
     * 
     * @param inputStream Byte stream
     * @return Read byte array
     * @throws IOException Thrown when reading fails
     */
    private byte[] readBinaryStreamAsBytes(InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, length);
            }
            return outputStream.toByteArray();
        }
    }
}

