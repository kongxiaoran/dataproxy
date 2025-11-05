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
package org.secretflow.dataproxy.integration.tests.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Dameng database test utility class.
 */
public class DamengTestUtil {

    private static final Properties properties = new Properties();

    static {
        try (InputStream is = DamengTestUtil.class.getResourceAsStream("/test-dameng.conf")) {
            if (is != null) {
                properties.load(is);
            }
        } catch (Exception e) {
            // If config file doesn't exist, use default values or environment variables
            System.err.println("Warning: Could not load test-dameng.conf, using environment variables or defaults");
        }
    }

    public static String getDamengEndpoint() {
        String endpoint = properties.getProperty("test.dameng.endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = System.getenv("TEST_DAMENG_ENDPOINT");
        }
        return endpoint != null ? endpoint : "localhost:5236";
    }

    public static String getDamengDatabase() {
        String database = properties.getProperty("test.dameng.database");
        if (database == null || database.isEmpty()) {
            database = System.getenv("TEST_DAMENG_DATABASE");
        }
        return database != null ? database : "SYSDBA";
    }

    public static String getDamengUsername() {
        String username = properties.getProperty("test.dameng.username");
        if (username == null || username.isEmpty()) {
            username = System.getenv("TEST_DAMENG_USERNAME");
        }
        return username != null ? username : "SYSDBA";
    }

    public static String getDamengPassword() {
        String password = properties.getProperty("test.dameng.password");
        if (password == null || password.isEmpty()) {
            password = System.getenv("TEST_DAMENG_PASSWORD");
        }
        return password != null ? password : "SYSDBA";
    }
}

