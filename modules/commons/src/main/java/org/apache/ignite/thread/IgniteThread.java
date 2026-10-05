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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/thread/IgniteThread.java
// Lesson 0.3 子集：命名规则（name-#N%instance%）、静态计数器、基础访问器。
// vendor 另有 stripe/plc/cachePool 等线程分类访问器（通信/IO 治理随章 3 接入）。

package org.apache.ignite.thread;

import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Grid 线程：以统一规则命名（线程 dump 可读性），供工厂与执行器创建。
 */
public class IgniteThread extends Thread {
    /** 全局静态计数器。 */
    private static final AtomicLong cntr = new AtomicLong();

    /** 实例名。 */
    private final String igniteInstanceName;

    /** 线程名。 */
    private final String threadName;

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param threadName 线程名。
     */
    public IgniteThread(@Nullable String igniteInstanceName, String threadName) {
        this(igniteInstanceName, threadName, new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                // 空实现。
            }
        });
    }

    /**
     * @param igniteInstanceName Ignite 实例名。
     * @param threadName 线程名。
     * @param r 任务体。
     */
    public IgniteThread(@Nullable String igniteInstanceName, String threadName, Runnable r) {
        super(r, createName(nextSeq(), threadName, igniteInstanceName));

        this.igniteInstanceName = igniteInstanceName;
        this.threadName = threadName;
    }

    /**
     * 取下一序号（静态原子计数器，与 vendor 一致）。
     */
    private static long nextSeq() {
        return cntr.incrementAndGet();
    }

    /**
     * 按 vendor 规则构造线程名。
     *
     * @param num 序号。
     * @param threadName 线程名。
     * @param igniteInstanceName 实例名。
     * @return 完整线程名。
     */
    protected static String createName(long num, String threadName, @Nullable String igniteInstanceName) {
        return threadName + "-#" + num + (igniteInstanceName != null ? '%' + igniteInstanceName + '%' : "");
    }

    /**
     * @return 本线程的 Ignite 实例名。
     */
    @Nullable public String igniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return 本线程的逻辑名（不含序号与实例名部分）。
     */
    public String threadName() {
        return threadName;
    }
}
