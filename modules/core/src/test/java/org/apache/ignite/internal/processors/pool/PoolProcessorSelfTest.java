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

// Lesson 0.3 自写：PoolProcessor 最小闭包切片（公共池创建/校验/执行/关闭）。

package org.apache.ignite.internal.processors.pool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * PoolProcessor 测试。
 */
public class PoolProcessorSelfTest {
    /**
     * @param poolSize 公共池大小（写入配置）。
     * @return 装配好 PoolProcessor 的 kernal context。
     */
    private GridTestKernalContext ctx(int poolSize) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("pool-test");
        cfg.setGridLogger(new NullLogger());
        cfg.setPublicThreadPoolSize(poolSize);

        return new GridTestKernalContext(cfg.getGridLogger(), cfg);
    }

    /**
     * 公共池按配置大小创建，可执行任务，线程命名带实例名后缀（vendor 命名约定）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testPublicPoolCreatedAndExecutes() throws Exception {
        GridTestKernalContext ctx = ctx(2);

        PoolProcessor pool = new PoolProcessor(ctx);

        ctx.add(pool);

        pool.start();

        try {
            assertNotNull(pool.getExecutorService());

            Future<String> fut = pool.getExecutorService().submit(() ->
                "ran-on:" + Thread.currentThread().getName());

            String threadName = fut.get();

            assertTrue("Thread name must carry ignite instance name suffix: " + threadName,
                threadName.contains("%pool-test"));

            assertEquals("ran-on:" + threadName.split(":", 2)[1], threadName);
        }
        finally {
            pool.stop(true);
        }
    }

    /**
     * 非法池大小在 start 时抛异常（validateThreadPoolSize）。
     */
    @Test
    public void testInvalidPoolSizeRejected() {
        for (int bad : new int[]{0, -1}) {
            GridTestKernalContext ctx = ctx(bad);

            PoolProcessor pool = new PoolProcessor(ctx);

            try {
                pool.start();

                fail("Pool size " + bad + " must be rejected");
            }
            catch (IgniteCheckedException e) {
                assertTrue(e.getMessage().contains("public"));
            }

            // start 失败后的 stop 必须幂等（kernal 失败回滚路径会调用）。
            try {
                pool.stop(true);
            }
            catch (IgniteCheckedException e) {
                fail("stop after failed start must be no-op: " + e);
            }
        }
    }

    /**
     * stop 关闭全部执行器（vendor stopExecutors0 停后置空引用，引用须停前取）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testStopShutsDownExecutor() throws Exception {
        GridTestKernalContext ctx = ctx(1);

        PoolProcessor pool = new PoolProcessor(ctx);

        ctx.add(pool);

        pool.start();

        java.util.concurrent.ExecutorService exec = pool.getExecutorService();

        final CountDownLatch done = new CountDownLatch(1);

        exec.execute(done::countDown);

        assertTrue(done.await(5, TimeUnit.SECONDS));

        pool.stop(true);

        assertTrue("Executor must be shut down after stop", exec.isShutdown());
    }
}
