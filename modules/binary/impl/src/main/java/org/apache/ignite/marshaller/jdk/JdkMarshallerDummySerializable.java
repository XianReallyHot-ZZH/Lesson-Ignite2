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

// 对应 vendor: vendors/ignite/modules/binary/impl/src/main/java/org/apache/ignite/marshaller/jdk/JdkMarshallerDummySerializable.java
// Lesson 0.3 子集：完整类（裸 Object 的序列化占位——ObjectOutputStream 对裸
// Object 会写特殊标记，换成等价 bean 保证流结构一致）。

package org.apache.ignite.marshaller.jdk;

import java.io.Serializable;

/**
 * 裸 {@code Object} 的序列化占位。
 */
final class JdkMarshallerDummySerializable implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;
}
