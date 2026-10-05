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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/jdk/JdkMarshaller.java
// Lesson 0.3 子集：完整接口（api 模块只有接口，实现在 impl 模块的 JdkMarshallerImpl——2.18 拆分原样）。

package org.apache.ignite.marshaller.jdk;

import org.apache.ignite.marshaller.Marshaller;

/**
 * 基于 JDK 序列化机制的 {@link Marshaller} 实现（标记接口，实现在 impl 模块）。
 */
public interface JdkMarshaller extends Marshaller {
}
