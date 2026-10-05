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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridKernalState.java
// Lesson 0.3 子集：完整枚举（DISCONNECTED 仅由 client 断连路径使用，随章 13 接入）。

package org.apache.ignite.internal;

import org.jetbrains.annotations.Nullable;

/**
 * kernal 生命周期状态。
 */
public enum GridKernalState {
    /** kernal 已启动。 */
    STARTED,

    /** kernal 启动中。 */
    STARTING,

    /** kernal 停止中。 */
    STOPPING,

    /** kernal 已断连（client 模式）。 */
    DISCONNECTED,

    /** kernal 已停止（亦为初始状态）。 */
    STOPPED;

    /** 枚举值缓存。 */
    private static final GridKernalState[] VALS = values();

    /**
     * 按序号还原枚举。
     *
     * @param ord 序号字节。
     * @return 枚举（越界返回 {@code null}）。
     */
    @Nullable public static GridKernalState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
