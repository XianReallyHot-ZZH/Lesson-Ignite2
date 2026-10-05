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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/pool/PoolProcessor.java
// Lesson 0.3 子集：公共池（execSvc）的创建/校验/访问/关闭。
// vendor 共 17 个池 + 饥饿检测（onKernalStart 挂周期任务）+ metrics/system view 注册 +
// SecurityAware 包装 + 插件 IoPool——按消费者课程逐池接入（svc/sys/striped 随通信章 3，
// datastream 随 11.9，qry 随章 8……）；metrics 注册随运维面（13.7）。
// 失败回滚注意：start 抛出前 execSvc 可能未赋值，stop 必须对半启动状态幂等。

package org.apache.ignite.internal.processors.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 * 线程池管理 processor。
 */
public class PoolProcessor extends GridProcessorAdapter {
    /** 公共执行器。 */
    private ThreadPoolExecutor execSvc;

    /**
     * @param ctx kernal 上下文。
     */
    public PoolProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        Thread.UncaughtExceptionHandler oomeHnd = ctx.uncaughtExceptionHandler();

        validateThreadPoolSize(cfg.getPublicThreadPoolSize(), "public");

        execSvc = createExecutorService(
            "pub",
            cfg.getIgniteInstanceName(),
            cfg.getPublicThreadPoolSize(),
            cfg.getPublicThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            oomeHnd);

        // 核心线程允许超时回收（无负载时不占线程）。
        execSvc.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        U.shutdownNow(getClass(), execSvc, log);

        execSvc = null;
    }

    /**
     * 公共池访问器（计算/闭包等绝大多数公共任务执行入口）。
     *
     * @return 公共执行器。
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * 创建执行器（vendor 工厂方法的非 security 包装形态）。
     *
     * @param threadNamePrefix 线程名前缀。
     * @param igniteInstanceName 实例名。
     * @param corePoolSize 核心线程数。
     * @param maxPoolSize 最大线程数。
     * @param keepAliveTime 空闲保活毫秒。
     * @param eHnd 未捕获异常处理器。
     * @return 新执行器。
     */
    private IgniteThreadPoolExecutor createExecutorService(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        Thread.UncaughtExceptionHandler eHnd
    ) {
        return new IgniteThreadPoolExecutor(
            threadNamePrefix,
            igniteInstanceName,
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            // LinkedBlockingQueue 使 max 线程数实际不生效（vendor 同款注释）。
            new LinkedBlockingQueue<>(),
            eHnd);
    }

    /**
     * 校验线程池大小为正。
     *
     * @param poolSize 配置值。
     * @param poolName 池名（错误消息用）。
     * @throws IgniteCheckedException 值非法时抛出。
     */
    private static void validateThreadPoolSize(int poolSize, String poolName)
        throws IgniteCheckedException {
        if (poolSize <= 0) {
            throw new IgniteCheckedException("Invalid " + poolName + " thread pool size" +
                " (must be greater than 0), actual value: " + poolSize);
        }
    }
}
