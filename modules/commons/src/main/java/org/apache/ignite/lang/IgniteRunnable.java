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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/lang/IgniteRunnable.java
// Lesson 0.3 子集：完整接口（可序列化 Runnable）。
// 消费者：GridAbsClosure 的父接口。

package org.apache.ignite.lang;

import java.io.Serializable;

/**
 * 可序列化的 {@link Runnable}。
 * <p>
 * 所有闭包类（{@code GridAbsClosure} 等）以此为基础，保证能跨节点/跨时间传递。
 */
public interface IgniteRunnable extends Runnable, Serializable {
    // 无附加方法。
}
