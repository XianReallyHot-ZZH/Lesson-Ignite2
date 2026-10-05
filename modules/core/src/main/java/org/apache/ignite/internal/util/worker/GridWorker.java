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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/util/worker/GridWorker.java
// Lesson 0.3 子集：可取消的命名 worker（run→body、cancel、join、注册表联动）。
// 未接入：心跳（updateHeartbeat/heartbeatTs）与阻塞段标记
//（blockingSectionBegin/End）——WorkersRegistry 心跳治理随运维面课；
// onCancel 首次取消请求回调与 onCancelledBeforeWorkerScheduled——随 failover 课（11.4）。

package org.apache.ignite.internal.util.worker;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * 后台 worker 基类：统一命名、统一取消、统一错误日志、统一注册表登记。
 */
public abstract class GridWorker implements Runnable {
    /** 实例名。 */
    private final String igniteInstanceName;

    /** worker 逻辑名。 */
    private final String name;

    /** 日志。 */
    protected final IgniteLogger log;

    /** worker 注册表（可为 {@code null}——最小闭包无注册表）。 */
    private final WorkersRegistry registry;

    /** 取消标志。 */
    protected final AtomicBoolean isCancelled = new AtomicBoolean();

    /** 运行线程（run 开始后可用）。 */
    private volatile Thread runner;

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param name worker 逻辑名。
     * @param log 日志。
     * @param registry worker 注册表（可为 {@code null}）。
     */
    protected GridWorker(@Nullable String igniteInstanceName, String name, IgniteLogger log,
        @Nullable WorkersRegistry registry) {
        this.igniteInstanceName = igniteInstanceName;
        this.name = name;
        this.log = log;
        this.registry = registry;
    }

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param name worker 逻辑名。
     * @param log 日志。
     */
    protected GridWorker(@Nullable String igniteInstanceName, String name, IgniteLogger log) {
        this(igniteInstanceName, name, log, null);
    }

    /** {@inheritDoc} */
    @Override public final void run() {
        if (runner != null)
            throw new IllegalStateException("Grid worker " + name() + " can be run only once.");

        if (isCancelled.get())
            return;

        runner = Thread.currentThread();

        if (registry != null)
            registry.register(this);

        try {
            body();
        }
        catch (IgniteInterruptedCheckedException e) {
            if (log != null && log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);
        }
        catch (InterruptedException e) {
            if (log != null && log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            // 保留中断标志。
            Thread.currentThread().interrupt();
        }
        // 兜底捕获：确保异常被记录且不杀死线程池里的线程（vendor 原语义）。
        catch (Throwable e) {
            logError(e);

            if (e instanceof Error)
                throw e;
        }
        finally {
            cleanup();

            if (registry != null)
                registry.unregister(this);

            // 置空 runner，防止后续操作影响已被线程池回收的线程。
            runner = null;
        }
    }

    /**
     * 记录 worker 异常退出（vendor 在此走 FailureProcessor——随其课程接入）。
     *
     * @param e 异常。
     */
    private void logError(Throwable e) {
        if (log != null)
            log.error("Grid worker failed abnormally: " + name(), e);
    }

    /**
     * worker 主体。
     *
     * @throws InterruptedException 被中断时抛出。
     * @throws IgniteInterruptedCheckedException 被中断（checked 形态）时抛出。
     */
    protected abstract void body() throws InterruptedException, IgniteInterruptedCheckedException;

    /**
     * 清理钩子（worker 正常退出前调用）。
     */
    protected void cleanup() {
        // 无操作。
    }

    /**
     * @return 运行线程（未开始为 {@code null}）。
     */
    @Nullable public final Thread runner() {
        return runner;
    }

    /**
     * @return Ignite 实例名。
     */
    public final String igniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return worker 逻辑名。
     */
    public final String name() {
        return name + (igniteInstanceName != null ? '%' + igniteInstanceName : "");
    }

    /**
     * 取消 worker（幂等）。
     */
    public void cancel() {
        if (isCancelled.compareAndSet(false, true)) {
            Thread r = runner;

            if (r != null && r.isAlive())
                r.interrupt();
        }
    }

    /**
     * 等待 worker 结束。
     *
     * @throws InterruptedException 等待中被中断时抛出。
     */
    public final void join() throws InterruptedException {
        Thread r = runner;

        if (r != null)
            r.join();
    }

    /**
     * @return 是否已取消。
     */
    public final boolean isCancelled() {
        return isCancelled.get();
    }

    /**
     * @return worker 是否已跑完（未开始视为未结束）。
     */
    public final boolean isDone() {
        Thread r = runner;

        return r != null && !r.isAlive();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridWorker [name=" + name() + ", cancelled=" + isCancelled.get() + ']';
    }
}
