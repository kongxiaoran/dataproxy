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
import java.math.BigDecimal;

/**
 * @author yuexie
 * @date 2024/11/1 20:25
 **/
@Slf4j
public class ByteValueVisitor implements ValueVisitor<Byte>{

    @Override
    public Byte visit(boolean value) {
        return value? (byte)1: (byte)0;
    }

    @Override
    public Byte visit(@Nonnull Short value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Integer value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Long value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Float value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Double value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull String value) {
        try {
            return Byte.valueOf(value);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse string '{}' as Byte, using 0", value);
            return (byte) 0;
        }
    }

    @Override
    public Byte visit(@Nonnull BigDecimal value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Object value) {
        // Directly Byte type, return directly
        if (value instanceof Byte byteValue) {
            return byteValue;
        }
        
        // Number type (including BigDecimal, Integer, Long, Short, Float, Double, etc.)
        if (value instanceof Number number) {
            return number.byteValue();
        }
        
        // String type, call dedicated visit(String) method
        if (value instanceof String stringValue) {
            return visit(stringValue);
        }

        // Other types: try to convert to string then parse
        return visit(value.toString());
    }

}
