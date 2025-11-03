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
 **/
public class ArrowUtil {

    public static ArrowType parseKusciaColumnType(String type) {
        // string integer float datetime timestamp
        return switch (type) {
            case "int8" -> Types.MinorType.TINYINT.getType();
            case "int16" -> Types.MinorType.SMALLINT.getType();
            case "int32" -> Types.MinorType.INT.getType();
            case "int64", "int" -> Types.MinorType.BIGINT.getType();
            case "uint8" -> Types.MinorType.UINT1.getType();
            case "uint16" -> Types.MinorType.UINT2.getType();
            case "uint32" -> Types.MinorType.UINT4.getType();
            case "uint64" -> Types.MinorType.UINT8.getType();
            case "float32" -> Types.MinorType.FLOAT4.getType();
            case "float64", "float" -> Types.MinorType.FLOAT8.getType();
            case "date32" -> Types.MinorType.DATEDAY.getType();
            case "date64" -> Types.MinorType.DATEMILLI.getType();
            case "bool" -> Types.MinorType.BIT.getType();
            case "string", "str" -> Types.MinorType.VARCHAR.getType();
            case "binary" -> Types.MinorType.VARBINARY.getType();
            default -> throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported field types: " + type);
        };
    }
}
