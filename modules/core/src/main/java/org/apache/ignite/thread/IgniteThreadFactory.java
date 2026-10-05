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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/thread/IgniteThreadFactory.java
// Lesson 0.3 子集：完整工厂（裁剪 GridIoPolicy 参数——IO 策略字节属通信章 3，
// 届时补 plc 构造器重载）。

package org.apache.ignite.thread;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * 创建 grid 线程的 {@link ThreadFactory} 实现。
 */
public class IgniteThreadFactory implements ThreadFactory {
    /** Ignite 实例名。 */
    private final String igniteInstanceName;

    /** 线程名前缀。 */
    private final String threadName;

    /** 线程序号发生器。 */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** 未捕获异常处理器。 */
    private final UncaughtExceptionHandler eHnd;

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param threadName 线程名。
     */
    public IgniteThreadFactory(@Nullable String igniteInstanceName, String threadName) {
        this(igniteInstanceName, threadName, null);
    }

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param threadName 线程名。
     * @param eHnd 未捕获异常处理器。
     */
    public IgniteThreadFactory(@Nullable String igniteInstanceName, String threadName,
        @Nullable UncaughtExceptionHandler eHnd) {
        this.igniteInstanceName = igniteInstanceName;
        this.threadName = threadName;
        this.eHnd = eHnd;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread thread = new IgniteThread(igniteInstanceName, threadName, r);

        if (eHnd != null)
            thread.setUncaughtExceptionHandler(eHnd);

        return thread;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteThreadFactory [igniteInstanceName=" + igniteInstanceName
            + ", threadName=" + threadName + ']';
    }
}
