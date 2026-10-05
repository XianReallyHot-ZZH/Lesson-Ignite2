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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/thread/IgniteThreadPoolExecutor.java
// Lesson 0.3 子集：以 IgniteThreadFactory 造线程的 ThreadPoolExecutor
//（vendor 版把线程工厂与 IO 策略绑定，plc 字段随通信章 3 补）。

package org.apache.ignite.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

/**
 * 以 {@link IgniteThreadFactory} 创建线程的线程池执行器。
 */
public class IgniteThreadPoolExecutor extends ThreadPoolExecutor {
    /** 线程序号（诊断输出用）。 */
    private final AtomicInteger threadNum = new AtomicInteger();

    /**
     * @param threadNamePrefix 线程名前缀。
     * @param igniteInstanceName Ignite 实例名。
     * @param corePoolSize 核心线程数。
     * @param maxPoolSize 最大线程数。
     * @param keepAliveTime 空闲保活时间。
     * @param workQ 工作队列。
     * @param eHnd 未捕获异常处理器。
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        @Nullable String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        @Nullable Thread.UncaughtExceptionHandler eHnd
    ) {
        this(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ,
            new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, eHnd));
    }

    /**
     * @param threadNamePrefix 线程名前缀。
     * @param igniteInstanceName Ignite 实例名。
     * @param corePoolSize 核心线程数。
     * @param maxPoolSize 最大线程数。
     * @param keepAliveTime 空闲保活时间。
     * @param workQ 工作队列。
     * @param threadFactory 线程工厂。
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        @Nullable String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        ThreadFactory threadFactory
    ) {
        this(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ,
            threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 全参构造（vendor 对齐：AbortPolicy 兜底拒绝）。
     *
     * @param threadNamePrefix 线程名前缀。
     * @param igniteInstanceName Ignite 实例名。
     * @param corePoolSize 核心线程数。
     * @param maxPoolSize 最大线程数。
     * @param keepAliveTime 空闲保活时间。
     * @param workQ 工作队列。
     * @param threadFactory 线程工厂。
     * @param reh 拒绝处理器。
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        @Nullable String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        ThreadFactory threadFactory,
        RejectedExecutionHandler reh
    ) {
        super(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS, workQ, threadFactory, reh);
    }

    /** {@inheritDoc} */
    @Override protected void beforeExecute(Thread t, Runnable r) {
        threadNum.incrementAndGet();
    }
}
