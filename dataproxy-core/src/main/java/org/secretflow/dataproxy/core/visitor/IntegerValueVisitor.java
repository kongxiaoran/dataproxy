/*
 * Copyright 2024 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.core.visitor;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @author yuexie
 * @date 2024/11/1 16:42
 **/
@Slf4j
public class IntegerValueVisitor implements ValueVisitor<Integer> {

    @Override
    public Integer visit(@Nonnull String value) {
        return Integer.valueOf(value);
    }


    @Override
    public Integer visit(@Nonnull Object value) {

        if (value instanceof Integer integer) {
            return this.visit(integer);
        } else if (value instanceof Date dateValue) {
            return this.visit(dateValue);
        } else if (value instanceof LocalDateTime localDateTime) {
            return this.visit(localDateTime);
        } else if (value instanceof ZonedDateTime zonedDateTime) {
            return this.visit(zonedDateTime);
        } else if (value instanceof LocalDate localDate) {
            return this.visit(localDate);
        }

        return Integer.valueOf(value.toString());
    }

    @Override
    public Integer visit(@Nonnull Long value) {
        return value.intValue();
    }

    @Override
    public Integer visit(@Nonnull Double value) {
        return value.intValue();
    }

    @Override
    public Integer visit(boolean value) {
        return value ? 1 : 0;
    }

    @Override
    public Integer visit(@Nonnull Float value) {
        return value.intValue();
    }

    @Override
    public Integer visit(@Nonnull Short value) {
        return value.intValue();
    }

    @Override
    public Integer visit(@Nonnull Integer value) {
        return value;
    }

    @Override
    public Integer visit(@Nonnull Date value) {
        // Handle java.sql.Time: Time32Vector needs milliseconds since midnight
        if (value instanceof Time sqlTime) {
            // java.sql.Time.getTime() returns milliseconds since Unix epoch
            // But Time32Vector needs milliseconds since midnight of the day
            // Convert to LocalTime then calculate milliseconds
            return (int) (sqlTime.toLocalTime().toNanoOfDay() / 1_000_000);
        }

        // Handle java.sql.Date: DateDayVector needs days since 1970-01-01
        if (value instanceof java.sql.Date sqlDate) {
            return (int) sqlDate.toLocalDate().toEpochDay();
        }

        // For java.util.Date, assume it's a date type, convert milliseconds to days
        return (int) (value.getTime() / (24 * 60 * 60 * 1000L));
    }

    @Override
    public Integer visit(@Nonnull LocalDate value) {
        return (int) value.toEpochDay();
    }
}
