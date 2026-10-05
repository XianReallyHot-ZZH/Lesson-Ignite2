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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/worker/WorkersRegistry.java
// Lesson 0.3 子集：worker 登记/摘除容器。vendor 另有 updateHeartbeat/onBeforeStop
// （停机时扫 hung worker 报告）与 failure 集成——随 FailureProcessor/运维面课接入。

package org.apache.ignite.internal.worker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;

/**
 * 本节点全部后台 worker 的注册表（停机治理与心跳检测的锚点）。
 */
public class WorkersRegistry {
    /** 已登记 worker。 */
    private final Set<GridWorker> workers = ConcurrentHashMap.newKeySet();

    /**
     * 登记 worker。
     *
     * @param w worker。
     */
    public void register(GridWorker w) {
        workers.add(w);
    }

    /**
     * 摘除 worker。
     *
     * @param w worker。
     */
    public void unregister(GridWorker w) {
        workers.remove(w);
    }

    /**
     * @return 当前登记的 worker 数。
     */
    public int size() {
        return workers.size();
    }

    /**
     * @return worker 集合（诊断用只读视图；vendor 无此访问器，测试断言用）。
     */
    public Set<GridWorker> workers() {
        return java.util.Collections.unmodifiableSet(workers);
    }

    /**
     * 停机前扫 hung worker（vendor 在此对每个 worker 检查心跳并告警）。
     *
     * @param log 日志（可为 {@code null}）。
     */
    public void onBeforeStop(@Nullable org.apache.ignite.IgniteLogger log) {
        // 心跳治理随运维面课接入。
    }
}
