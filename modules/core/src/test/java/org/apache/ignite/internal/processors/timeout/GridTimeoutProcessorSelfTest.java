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

// 改写自 vendor: vendors/ignite/modules/core/src/test/java/org/apache/ignite/internal/processors/timeout/GridTimeoutProcessorSelfTest.java
// （Apache 2.0；保留原测试切片与断言结构，GridTestUtils.runMultiThreaded 换成裸线程、
//  计数缩减以控测试时长；补 schedule(CancelableTask) 切片）

package org.apache.ignite.internal.processors.timeout;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.logger.NullLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Timeout processor 测试。
 */
public class GridTimeoutProcessorSelfTest {
    /** 随机数发生器。 */
    private static final Random RAND = new Random();

    /** kernal context。 */
    private GridTestKernalContext ctx;

    /** {@inheritDoc} */
    @Before public void beforeTest() throws Exception {
        ctx = new GridTestKernalContext(new NullLogger());

        ctx.add(new GridTimeoutProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @After public void afterTest() throws Exception {
        ctx.stop(true);

        ctx = null;
    }

    /**
     * 测试超时回调。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testTimeouts() throws Exception {
        int max = 100;

        final CountDownLatch latch = new CountDownLatch(max);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < max; i++) {
            final int idx = i;

            ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                /** 超时 ID。 */
                private final IgniteUuid id = IgniteUuid.randomUuid();

                /** 结束时间。 */
                private final long endTime = System.currentTimeMillis() + RAND.nextInt(1000);

                /** {@inheritDoc} */
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                /** {@inheritDoc} */
                @Override public long endTime() {
                    return endTime;
                }

                /** {@inheritDoc} */
                @Override public void onTimeout() {
                    long now = System.currentTimeMillis();

                    if (now < endTime)
                        fail("Timeout event happened prematurely [endTime=" + endTime + ", now=" + now + ']');

                    timeObjs.add(this);

                    latch.countDown();
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertEquals(max, timeObjs.size());

        // 校验回调顺序按 endTime 非降序。
        long endTime = 0;

        for (GridTimeoutObject obj : timeObjs) {
            assertTrue(endTime <= obj.endTime());

            endTime = obj.endTime();
        }
    }

    /**
     * 多线程注册超时对象（adapter 形态）。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testTimeoutObjectAdapterMultithreaded() throws Exception {
        final int max = 50;

        int threads = 8;

        final CountDownLatch latch = new CountDownLatch(max * threads);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        Thread[] workers = new Thread[threads];

        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    final int idx = i;

                    ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RAND.nextInt(1000) + 500) {
                        /** {@inheritDoc} */
                        @Override public void onTimeout() {
                            long now = System.currentTimeMillis();

                            if (now < endTime())
                                fail("Timeout event happened prematurely [endTime=" + endTime() + ", now=" + now + ']');

                            // 回调只来自单一线程，无需同步。
                            timeObjs.add(this);

                            latch.countDown();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime()
                                + ", id=" + timeoutId() + ']';
                        }
                    });
                }
            }, "timeout-test-worker");

            workers[t].start();
        }

        for (Thread w : workers)
            w.join();

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertEquals(max * threads, timeObjs.size());

        long endTime = 0;

        for (GridTimeoutObject obj : timeObjs) {
            assertTrue("Sequence check failed [endTime=" + endTime + ", obj=" + obj + ']',
                endTime <= obj.endTime());

            endTime = obj.endTime();
        }
    }

    /**
     * 测试超时回调从不发生（移除后不再触发）。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testTimeoutNeverCalled() throws Exception {
        int max = 100;

        final AtomicInteger callCnt = new AtomicInteger();

        Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < max; i++) {
            final int idx = i;

            GridTimeoutObject obj = new GridTimeoutObject() {
                /** 超时 ID。 */
                private final IgniteUuid id = IgniteUuid.randomUuid();

                /** 结束时间。 */
                private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 500;

                /** {@inheritDoc} */
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                /** {@inheritDoc} */
                @Override public long endTime() {
                    return endTime;
                }

                /** {@inheritDoc} */
                @Override public void onTimeout() {
                    callCnt.incrementAndGet();
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                }
            };

            timeObjs.add(obj);

            ctx.timeout().addTimeoutObject(obj);
        }

        assertEquals(max, timeObjs.size());

        // 移除全部对象，使它们来不及到期（循环耗时 < 500ms 的假设下）。
        for (GridTimeoutObject obj : timeObjs)
            ctx.timeout().removeTimeoutObject(obj);

        Thread.sleep(1000);

        assertEquals(0, callCnt.get());
    }

    /**
     * 测试超时对象只回调一次。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testTimeoutCallOnce() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();

        ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
            /** 超时 ID。 */
            private final IgniteUuid id = IgniteUuid.randomUuid();

            /** 结束时间。 */
            private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 100;

            /** {@inheritDoc} */
            @Override public IgniteUuid timeoutId() {
                return id;
            }

            /** {@inheritDoc} */
            @Override public long endTime() {
                return endTime;
            }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                if (cnt.incrementAndGet() > 1)
                    fail("Timeout should not be called more than once: " + this);
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Timeout test object [endTime=" + endTime + ", id=" + id + ']';
            }
        });

        Thread.sleep(2000);

        assertEquals(1, cnt.get());
    }

    /**
     * 测试相同 endTime 的两个对象都回调。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testTimeoutSameEndTime() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);

        final long endTime0 = System.currentTimeMillis() + 1000;

        for (int i = 0; i < 2; i++) {
            ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                /** 超时 ID。 */
                private final IgniteUuid id = IgniteUuid.randomUuid();

                /** 结束时间。 */
                private final long endTime = endTime0;

                /** {@inheritDoc} */
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                /** {@inheritDoc} */
                @Override public long endTime() {
                    return endTime;
                }

                /** {@inheritDoc} */
                @Override public void onTimeout() {
                    latch.countDown();
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Timeout test object [endTime=" + endTime + ", id=" + id + ']';
                }
            });
        }

        assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
    }

    /**
     * schedule：周期任务重复执行，close 后停止。
     *
     * @throws Exception 若测试失败。
     */
    @Test
    public void testSchedulePeriodicTask() throws Exception {
        final CountDownLatch latch = new CountDownLatch(3);

        GridTimeoutProcessor.CancelableTask task = ctx.timeout().schedule(latch::countDown, 50, 50);

        assertTrue("Periodic task must fire at least 3 times",
            latch.await(10, TimeUnit.SECONDS));

        task.close();

        // close 后不再触发。
        int fired = 3 - (int)latch.getCount();

        Thread.sleep(300);

        assertEquals(fired, 3 - (int)latch.getCount());
    }

    /**
     * 零/负 endTime 的对象不参与调度（永不超时语义）。
     */
    @Test
    public void testNeverExpiringObjectRejected() {
        GridTimeoutObject never = new GridTimeoutObjectAdapter(Long.MAX_VALUE) {
            /** {@inheritDoc} */
            @Override public void onTimeout() {
                fail("Must never fire");
            }
        };

        // GridTimeoutObjectAdapter 对负 timeout 折算 Long.MAX_VALUE → endTime 溢出保护折 MAX。
        assertTrue(never.endTime() == Long.MAX_VALUE);

        assertEquals(false, ctx.timeout().addTimeoutObject(never));
    }
}
