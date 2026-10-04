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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/IgnitionListener.java
// Lesson 0.2 子集：完整复刻（单方法接口即 vendor 全部成员）。

package org.apache.ignite;

import java.util.EventListener;
import org.jetbrains.annotations.Nullable;

/**
 * Ignition 工厂生命周期事件监听器。
 * <p>
 * 与 Ignite 其他监听器不同，本监听器由触发状态变更的同一个线程回调——
 * 监听器逻辑应保持轻量，并自行捕获可能的运行时异常。
 */
public interface IgnitionListener extends EventListener {
    /**
     * Ignite 实例状态变更回调。
     *
     * @param name 实例名（默认实例为 {@code null}）。
     * @param state 新状态。
     */
    public void onStateChange(@Nullable String name, IgniteState state);
}
