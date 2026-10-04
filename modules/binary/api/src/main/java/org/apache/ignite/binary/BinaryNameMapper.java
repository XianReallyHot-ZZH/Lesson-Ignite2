/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/binary/BinaryNameMapper.java
// Lesson 0.1 子集：完整复刻（纯接口）。默认实现 BinaryBasicNameMapper 随章 3 binary 课填充。

package org.apache.ignite.binary;

/**
 * Maps type and field names to different names. Prepares class/type names
 * and field names before pass them to {@link BinaryIdMapper}.
 * <p>
 * Binary name mapper can be configured for all binary objects via
 * {@code BinaryConfiguration#getNameMapper()} method,
 * or for a specific binary type via {@code BinaryTypeConfiguration#getNameMapper()} method.
 *
 * @see BinaryIdMapper
 */
public interface BinaryNameMapper {
    // 【教学】binary 类型标识映射链的第一环：NameMapper 先把类名/字段名"整形"
    // （如下划线化），IdMapper 再把整形后的名字算成 typeId/fieldId。这对接口
    // 将被 3.10/13 课的 BinaryMarshaller 消费——跨节点 typeId 一致性的起点。
    /**
     * Gets type clsName.
     *
     * @param clsName Class came
     * @return Type name.
     */
    String typeName(String clsName);

    /**
     * Gets field name.
     *
     * @param fieldName Field name.
     * @return Field name.
     */
    String fieldName(String fieldName);
}
