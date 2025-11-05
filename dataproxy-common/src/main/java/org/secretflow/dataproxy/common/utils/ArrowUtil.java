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

package org.secretflow.dataproxy.common.utils;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;

/**
 * @author yuexie
 * @date 2024/11/8 15:51
 * Arrow type utility for parsing Kuscia column types to Arrow types
 **/
public class ArrowUtil {

    /**
     * Parse Kuscia column type to Arrow type
     * @param type Column type string (e.g., "int32", "interval_year_month", "large_string")
     * @return ArrowType
     */
    public static ArrowType parseKusciaColumnType(String type) {
        String typeLower = type.toLowerCase();
        return switch (typeLower) {
            // Integer types
            case "int8" -> Types.MinorType.TINYINT.getType();
            case "int16" -> Types.MinorType.SMALLINT.getType();
            case "int32" -> Types.MinorType.INT.getType();
            case "int64", "int" -> Types.MinorType.BIGINT.getType();
            case "uint8" -> Types.MinorType.UINT1.getType();
            case "uint16" -> Types.MinorType.UINT2.getType();
            case "uint32" -> Types.MinorType.UINT4.getType();
            case "uint64" -> Types.MinorType.UINT8.getType();
            
            // Floating point types
            case "float32" -> Types.MinorType.FLOAT4.getType();
            case "float64", "float" -> Types.MinorType.FLOAT8.getType();
            
            // Date types
            case "date32" -> Types.MinorType.DATEDAY.getType();
            case "date64" -> Types.MinorType.DATEMILLI.getType();
            
            // Time types
            case "time32" -> Types.MinorType.TIMEMILLI.getType();
            case "time64" -> Types.MinorType.TIMEMICRO.getType();
            
            // Timestamp types
            case "timestamp" -> Types.MinorType.TIMESTAMPMICRO.getType();
            case "timestamp_us" -> Types.MinorType.TIMESTAMPMICRO.getType();
            case "timestamp_ms" -> Types.MinorType.TIMESTAMPMILLI.getType();
            case "timestamp_ns" -> Types.MinorType.TIMESTAMPNANO.getType();
            case "timestamp_tz" -> Types.MinorType.TIMESTAMPMICROTZ.getType();
            
            // Boolean types
            case "bool" -> Types.MinorType.BIT.getType();
            
            // String types
            case "string", "str" -> Types.MinorType.VARCHAR.getType();
            case "large_string", "large_utf8", "utf8_large" -> Types.MinorType.LARGEVARCHAR.getType();
            
            // Binary types
            case "binary" -> Types.MinorType.VARBINARY.getType();
            case "large_binary", "large_varbinary", "varbinary_large" -> Types.MinorType.LARGEVARBINARY.getType();
            
            // Decimal types
            // Note: Types.MinorType.DECIMAL.getType() throws UnsupportedOperationException
            // Decimal requires precision/scale, must use new ArrowType.Decimal(precision, scale, bitWidth)
            case "decimal" -> new ArrowType.Decimal(38, 10, 128);
            
            // Interval types
            case "interval_year_month", "interval_ym" -> 
                Types.MinorType.INTERVALYEAR.getType();
            case "interval_day_time", "interval_dt" -> 
                Types.MinorType.INTERVALDAY.getType();
            case "interval" -> Types.MinorType.INTERVALYEAR.getType();
            
            default -> throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported field types: " + type);
        };
    }
}
