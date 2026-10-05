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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridComponent.java
// Lesson 0.3 子集：生命周期四方法（start/stop/onKernalStart/onKernalStop）+ printMemoryStats。
// 未接入方法（按归属课程排列，接入时按 vendor 原签名补）：
// - DiscoveryDataExchangeType 枚举与 discoveryDataType()/collectJoiningNodeData()/
//   collectGridNodeData()/onGridDataReceived()/onJoiningNodeDataReceived()/validateNode()×2
//   ——discovery 数据交换，随章 2（ring 拓扑）接入；
// - onDisconnected()/onReconnected() ——client 断连/重连，随章 13 接入。

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;

/**
 * 全部内部组件（manager 与 processor）的公共契约。
 * <p>
 * 生命周期由 kernal 编排：<b>先 {@code ctx.add} 注册后 {@code start()}</b>——避免
 * "组件已启动但注册表查不到"的窗口；启动完成后再统一回调 {@link #onKernalStart(boolean)}。
 */
public interface GridComponent {
    /**
     * 启动组件。
     *
     * @throws IgniteCheckedException 任何错误时抛出。
     */
    public void start() throws IgniteCheckedException;

    /**
     * 停止组件。
     *
     * @param cancel 为 {@code true} 时相关组件须取消在途任务/作业。
     * @throws IgniteCheckedException 任何错误时抛出。
     */
    public void stop(boolean cancel) throws IgniteCheckedException;

    /**
     * kernal 成功启动（全部 manager/processor 已启动）后的回调。
     *
     * @param active 集群 active 标志（状态可能并发变化，谨慎使用）。
     * @throws IgniteCheckedException 任何错误时抛出。
     */
    public void onKernalStart(boolean active) throws IgniteCheckedException;

    /**
     * kernal 即将停止的回调。
     *
     * @param cancel 是否取消运行中的作业。
     */
    public void onKernalStop(boolean cancel);

    /**
     * 打印内存统计（内部结构大小等，仅测试与 profiling 用）。
     */
    public void printMemoryStats();
}
