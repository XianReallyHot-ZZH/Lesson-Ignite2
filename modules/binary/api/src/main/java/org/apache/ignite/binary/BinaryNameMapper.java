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
 * 把类型名与字段名映射为不同的名字。在把类/类型名与字段名传给 {@link BinaryIdMapper}
 * 之前先做预处理。
 * <p>
 * 名称映射器可通过 {@code BinaryConfiguration#getNameMapper()} 为全部 binary 对象配置，
 * 或通过 {@code BinaryTypeConfiguration#getNameMapper()} 为特定 binary 类型配置。
 *
 * @see BinaryIdMapper
 */
public interface BinaryNameMapper {
    /**
     * 获取类型名。
     *
     * @param clsName 传入的类名。
     * @return 类型名。
     */
    String typeName(String clsName);

    /**
     * 获取字段名。
     *
     * @param fieldName 字段名。
     * @return 字段名。
     */
    String fieldName(String fieldName);
}
