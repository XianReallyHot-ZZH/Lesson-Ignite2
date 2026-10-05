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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridKernalGatewayImpl.java
// Lesson 0.3 子集：读写锁 + AtomicReference 状态 + 用户栈踪迹 + busy-wait 写锁。
// 差异：
// - vendor 用 StripedCompositeReadWriteLock（按 CPU 数分条的复合读写锁，降低读锁竞争），
//   复刻用普通 ReentrantReadWriteLock（语义等价，分条优化随通信章压力测试再评估）；
// - DISCONNECTED 分支与 reconnect future（onDisconnected/onReconnected）未接入（13.4）。

package org.apache.ignite.internal;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteIllegalStateException;
import org.jetbrains.annotations.Nullable;

/**
 * kernal gateway 默认实现。
 */
public class GridKernalGatewayImpl implements GridKernalGateway, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** 防止 kernal 停止期间仍有 kernal 调用在跑的读写锁。 */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** 状态（AtomicReference 支持后续 client 断连的 CAS 迁移）。 */
    private final AtomicReference<GridKernalState> state = new AtomicReference<>(GridKernalState.STOPPED);

    /** 实例名。 */
    private final String igniteInstanceName;

    /**
     * 用户栈踪迹（首次公共 API 调用捕获）。
     * 刻意非 volatile——纯诊断用途（vendor 同款优化注释）。
     */
    private String stackTrace;

    /**
     * @param igniteInstanceName Ignite 实例名。
     */
    public GridKernalGatewayImpl(@Nullable String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void readLock() throws IgniteIllegalStateException {
        if (stackTrace == null)
            stackTrace = stackTrace();

        Lock lock = rwLock.readLock();

        lock.lock();

        GridKernalState state = this.state.get();

        if (state != GridKernalState.STARTED) {
            // 释放刚获取的锁再抛出。
            lock.unlock();

            throw illegalState();
        }
    }

    /** {@inheritDoc} */
    @Override public void readLockAnyway() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock().lock();
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        rwLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void writeLock() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        boolean interrupted = false;

        // 刻意 busy-wait（vendor 原样）。
        while (true)
            try {
                if (rwLock.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // 保留中断标志并继续——写锁必须拿到（vendor 原注释：标志会被清除）。
                interrupted = true;
            }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /** {@inheritDoc} */
    @Override public boolean tryWriteLock(long timeout) throws InterruptedException {
        boolean acquired = rwLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS);

        if (acquired) {
            if (stackTrace == null)
                stackTrace = stackTrace();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock() {
        rwLock.writeLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public void setState(GridKernalState state) {
        assert state != null;

        // 注意：本方法必须始终在写锁内调用。
        this.state.set(state);
    }

    /** {@inheritDoc} */
    @Override public GridKernalState getState() {
        return state.get();
    }

    /** {@inheritDoc} */
    @Override public String userStackTrace() {
        return stackTrace;
    }

    /**
     * 捕获用户栈踪迹。
     *
     * @return 栈踪迹字符串。
     */
    private static String stackTrace() {
        StringWriter sw = new StringWriter();

        new Throwable().printStackTrace(new PrintWriter(sw));

        return sw.toString();
    }

    /**
     * 构造非法状态异常。
     *
     * @return 新异常。
     */
    private IgniteIllegalStateException illegalState() {
        return new IgniteIllegalStateException("Grid is in invalid state to perform this operation. " +
            "It either not started yet or has already being or have stopped [igniteInstanceName=" + igniteInstanceName +
            ", state=" + state + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridKernalGatewayImpl [igniteInstanceName=" + igniteInstanceName
            + ", state=" + state.get() + ']';
    }
}
