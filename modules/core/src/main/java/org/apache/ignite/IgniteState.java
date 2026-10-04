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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/IgniteState.java
// Lesson 0.1 子集：完整复刻。0.2/0.3 课的 Ignition 注册表/gateway 状态机将以它为准绳。

package org.apache.ignite;

import org.jetbrains.annotations.Nullable;

/**
 * {@link org.apache.ignite.Ignition} 的可能状态。可通过
 * {@link org.apache.ignite.Ignition#addListener(IgnitionListener)} 方法注册状态变更通知监听器。
 */
public enum IgniteState {
    /**
     * Grid 工厂已启动。
     */
    STARTED,

    /**
     * Grid 工厂已停止。
     */
    STOPPED,

    /**
     * Grid 工厂因网络分段问题而停止。
     * <p>
     * 仅当分段策略设为 {@code STOP} 或 {@code RESTART_JVM}、且节点在分段失效后
     * 被 Ignite 内部停止时，才会触发此状态的通知。
     */
    STOPPED_ON_SEGMENTATION,

    /**
     * Grid 工厂因致命失败而停止。
     */
    STOPPED_ON_FAILURE;

    /** 枚举值数组。 */
    private static final IgniteState[] VALS = values();

    /**
     * 按序数高效获取枚举值。
     *
     * @param ord 序数值。
     * @return 枚举值。
     */
    @Nullable public static IgniteState fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
