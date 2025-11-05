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

/**
 * Dameng database Flight Producer implementation.
 *
 * <p>This producer extends AbstractDatabaseFlightProducer and provides
 * Dameng-specific implementations for connection initialization, SQL building,
 * and type conversion.</p>
 *
 */
package org.secretflow.dataproxy.producer;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.producer.AbstractDatabaseFlightProducer;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseDoGetContext;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;
import org.secretflow.dataproxy.util.DamengUtil;

/**
 * @author: kongxiaoran
 * @date: 2025/11/5
 */
public class DamengFlightProducer extends AbstractDatabaseFlightProducer {

    /**
     * Returns the producer name used for SPI registration.
     *
     * @return "dameng"
     */
    @Override
    public String getProducerName() {
        return "dameng";
    }

    /**
     * Initializes the database read context with Dameng-specific implementations.
     *
     * @param config Command configuration
     * @return DatabaseDoGetContext for reading data
     */
    @Override
    protected DatabaseDoGetContext initDoGetContext(DatabaseCommandConfig<?> config) {
        /*
         * Initialize database read context with Dameng-specific implementations:
         * 1. Initialize JDBC connection
         * 2. Build SELECT SQL
         * 3. Convert JDBC types to Arrow types
         */
        return new DatabaseDoGetContext(
                config,
                DamengUtil::initDameng,
                DamengUtil::buildQuerySql,
                DamengUtil::jdbcType2ArrowType
        );
    }

    /**
     * Initializes the database write context with Dameng-specific implementations.
     *
     * @param config Write configuration
     * @return DatabaseRecordWriter for writing data
     */
    @Override
    protected DatabaseRecordWriter initRecordWriter(DatabaseWriteConfig config) {
        /*
         * Initialize database write context with Dameng-specific implementations:
         * 1. Initialize JDBC connection
         * 2. Build CREATE TABLE SQL
         * 3. Build batch INSERT SQL
         * 4. Check if table exists
         */
        return new DatabaseRecordWriter(
                config,
                DamengUtil::initDameng,
                DamengUtil::buildCreateTableSql,
                DamengUtil::buildMultiRowInsertSql,
                DamengUtil::checkTableExists
        );
    }
}
