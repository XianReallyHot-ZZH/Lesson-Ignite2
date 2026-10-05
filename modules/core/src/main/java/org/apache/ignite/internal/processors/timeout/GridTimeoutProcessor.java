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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/timeout/GridTimeoutProcessor.java
// Lesson 0.3 子集：时间排序集 + 单 worker 调度 + CancelableTask 周期任务。
// 未接入：waitAsync（future + 超时闭包桥接，依赖 IgniteInternalFuture——随 future
// 基础设施课接入）；worker 心跳（updateHeartbeat）与 FailureProcessor 终局上报
//（CRITICAL_ERROR/SYSTEM_WORKER_TERMINATION——随 FailureProcessor 课接入）。

package org.apache.ignite.internal.processors.timeout;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteUuid;

/**
 * 检测并处理超时事件。
 */
public class GridTimeoutProcessor extends GridProcessorAdapter {
    /** 超时 worker。 */
    private final TimeoutWorker timeoutWorker;

    /** 超时对象的时间有序集合。 */
    private final GridConcurrentSkipListSet<GridTimeoutObject> timeoutObjs =
        new GridConcurrentSkipListSet<>(new Comparator<GridTimeoutObject>() {
            /** {@inheritDoc} */
            @Override public int compare(GridTimeoutObject o1, GridTimeoutObject o2) {
                int res = Long.compare(o1.endTime(), o2.endTime());

                if (res != 0)
                    return res;

                res = o1.timeoutId().compareTo(o2.timeoutId());

                if (res != 0)
                    return res;

                // 不同子系统的超时与 ID 可能相交，最终以类名定序。
                return o1.getClass().getName().compareTo(o2.getClass().getName());
            }
        });

    /** worker 唤醒互斥。 */
    private final Object mux = new Object();

    /**
     * @param ctx kernal 上下文。
     */
    public GridTimeoutProcessor(GridKernalContext ctx) {
        super(ctx);

        timeoutWorker = new TimeoutWorker(ctx.workersRegistry());
    }

    /** {@inheritDoc} */
    @Override public void start() {
        U.newThread(timeoutWorker, "grid-timeout-worker").start();

        if (log.isDebugEnabled())
            log.debug("Timeout processor started.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        timeoutWorker.cancel();

        try {
            timeoutWorker.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }

        if (log.isDebugEnabled())
            log.debug("Timeout processor stopped.");
    }

    /**
     * @param timeoutObj 超时对象。
     * @return 成功加入返回 {@code true}（永不超时的对象被拒绝）。
     */
    @SuppressWarnings({"NakedNotify", "CallToNotifyInsteadOfNotifyAll"})
    public boolean addTimeoutObject(GridTimeoutObject timeoutObj) {
        if (timeoutObj.endTime() <= 0 || timeoutObj.endTime() == Long.MAX_VALUE)
            // 永不触发的超时。
            return false;

        boolean added = timeoutObjs.add(timeoutObj);

        assert added : "Duplicate timeout object found: " + timeoutObj;

        if (timeoutObjs.firstx() == timeoutObj) {
            synchronized (mux) {
                mux.notify(); // 只有一个等待线程，notify 足够。
            }
        }

        return true;
    }

    /**
     * 调度定时任务（可取消、可周期）。
     *
     * @param task 任务。
     * @param delay 首次执行延迟毫秒。
     * @param period 执行周期毫秒（-1 表示单次）。
     * @return 可取消句柄。
     */
    public CancelableTask schedule(Runnable task, long delay, long period) {
        assert delay >= 0 : delay;
        assert period > 0 || period == -1 : period;

        CancelableTask obj = new CancelableTask(task, U.currentTimeMillis() + delay, period);

        addTimeoutObject(obj);

        return obj;
    }

    /**
     * @param timeoutObj 超时对象。
     * @return 成功移除返回 {@code true}。
     */
    public boolean removeTimeoutObject(GridTimeoutObject timeoutObj) {
        return timeoutObjs.remove(timeoutObj);
    }

    /**
     * 处理超时的 worker。
     */
    private class TimeoutWorker extends GridWorker {
        /**
         * @param registry worker 注册表（可为 {@code null}）。
         */
        TimeoutWorker(WorkersRegistry registry) {
            super(
                ctx.config().getIgniteInstanceName(),
                "grid-timeout-worker",
                GridTimeoutProcessor.this.log,
                registry
            );
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isCancelled()) {
                long now = U.currentTimeMillis();

                for (Iterator<GridTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext(); ) {
                    GridTimeoutObject timeoutObj = iter.next();

                    if (timeoutObj.endTime() <= now) {
                        try {
                            boolean rmvd = timeoutObjs.remove(timeoutObj);

                            if (log.isDebugEnabled())
                                log.debug("Timeout has occurred [obj=" + timeoutObj + ", process=" + rmvd + ']');

                            if (rmvd)
                                timeoutObj.onTimeout();
                        }
                        catch (Throwable e) {
                            if (isCancelled() && !(e instanceof Error)) {
                                if (log.isDebugEnabled())
                                    log.debug("Error when executing timeout callback: " + timeoutObj);

                                return;
                            }

                            U.error(log, "Error when executing timeout callback: " + timeoutObj, e);

                            if (e instanceof Error)
                                throw e;
                        }
                    }
                    else
                        break;
                }

                synchronized (mux) {
                    while (!isCancelled()) {
                        // 首元素访问必须在同步块内——避免错过 addTimeoutObject 的唤醒。
                        GridTimeoutObject first = timeoutObjs.firstx();

                        if (first != null) {
                            long waitTime = first.endTime() - U.currentTimeMillis();

                            if (waitTime > 0)
                                mux.wait(waitTime);
                            else
                                break;
                        }
                        else
                            mux.wait(5000);
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Timeout processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   timeoutObjsSize: " + timeoutObjs.size());
    }

    /**
     * 可取消的定时任务。
     */
    public class CancelableTask implements GridTimeoutObject, Closeable {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private long endTime;

        /** */
        private final long period;

        /** */
        private volatile boolean cancel;

        /** */
        private final Runnable task;

        /**
         * @param task 任务。
         * @param firstTime 首次触发时间（绝对毫秒）。
         * @param period 周期毫秒（-1 单次）。
         */
        CancelableTask(Runnable task, long firstTime, long period) {
            this.task = task;
            endTime = firstTime;
            this.period = period;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public synchronized void onTimeout() {
            if (cancel)
                return;

            try {
                task.run();
            }
            finally {
                if (!cancel && period > 0) {
                    endTime = U.currentTimeMillis() + period;

                    addTimeoutObject(this);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            cancel = true;

            synchronized (this) {
                // 等待本次执行结束，确保任务不再被触发。
                removeTimeoutObject(this);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CancelableTask [id=" + id + ", endTime=" + endTime + ", period=" + period + ']';
        }
    }
}
