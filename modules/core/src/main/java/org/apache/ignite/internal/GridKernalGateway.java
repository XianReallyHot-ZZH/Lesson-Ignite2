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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridKernalGateway.java
// Lesson 0.3 子集：读写锁准入 + 状态读写 + 用户栈踪迹 + tryWriteLock。
// vendor 另有 onDisconnected()/onReconnected()（reconnect future 桥接，
// 依赖 IgniteClientDisconnectedException/GridFutureAdapter——随 thick client 课 13.4 接入）。

package org.apache.ignite.internal;

/**
 * 本接口守卫对 kernal 功能的公共 API 访问：公共方法进入时 {@link #readLock()}，
 * 离开时 {@link #readUnlock()}；状态迁移（start/stop）持有 {@link #writeLock()}。
 * <p>
 * 注意：{@link org.apache.ignite.Ignite} 与 {@code ClusterNode} 的实现自身已有管理，
 * 不经本 gateway 守卫；守卫的是 {@code ClusterGroup} 一类的"富接口"实现。
 */
public interface GridKernalGateway {
    /**
     * 每个直接或间接经公共 API 发起的 kernal 调用进入时应调用本方法。
     * <p>
     * 本方法实质是获取读锁——多线程可同时进入而不阻塞。
     *
     * @throws IllegalStateException kernal 调用不被允许时抛出。
     * @see #readUnlock()
     */
    public void readLock() throws IllegalStateException;

    /**
     * 与 {@link #readLock()} 相同，但 grid 已停止时不抛 IllegalStateException。
     */
    public void readLockAnyway();

    /**
     * 设置 kernal 状态。不同状态驱动 gateway 内部逻辑。
     * <p>
     * 必须在 {@link #writeLock()} 持有期间调用。
     *
     * @param state 目标状态。
     */
    public void setState(GridKernalState state);

    /**
     * @return 当前 kernal 状态。
     */
    public GridKernalState getState();

    /**
     * 每个直接或间接经公共 API 发起的 kernal 调用离开时应调用本方法——
     * 释放 {@link #readLock()} 获取的读锁。
     *
     * @see #readLock()
     */
    public void readUnlock();

    /**
     * 等待所有在途调用退出并阻塞后续 {@link #readLock()}，直到 {@link #writeUnlock()}。
     * <p>
     * 本方法实质是获取内部写锁。
     */
    public void writeLock();

    /**
     * 解除 {@link #writeLock()} 的阻塞。
     */
    public void writeUnlock();

    /**
     * @return 首次 grid 公共 API 调用者的用户栈踪迹（诊断"谁在停机时还持着读锁"）。
     */
    public String userStackTrace();

    /**
     * 限时尝试获取写锁。
     *
     * @param timeout 超时毫秒数。
     * @return 获取成功返回 {@code true}。
     * @throws InterruptedException 被中断时抛出。
     */
    public boolean tryWriteLock(long timeout) throws InterruptedException;
}
