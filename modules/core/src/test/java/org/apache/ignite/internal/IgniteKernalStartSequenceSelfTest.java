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

// Lesson 0.3 tracer 测试（自写，course-map §0.3 验收来源）：
// Ignition.start() 返回且 state==STARTED + kernal 启动序列 == vendor §6 时间线子序列（生长不变量）。

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.logger.NullLogger;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Lesson 0.3 tracer：kernal 容器与最小闭包。
 */
public class IgniteKernalStartSequenceSelfTest {
    /** 实例名（每测试方法内 Ignition 状态隔离）。 */
    private static final String NAME = "kernal-seq-test";

    /** vendor §6 启动时间线里各组件的序号（与 docs/research/01-startup-path.md §5
     * processor 表逐项对应：PoolProcessor=第 8 个启动的 processor，GridTimeoutProcessor=第 12 个）。
     * 新课往 kernal 加组件时，必须在此表登记其 vendor 时间线序号——未登记即测试失败，
     * 这是"复刻启动序列恒为 vendor 时间线子序列"的生长不变量守卫。
     * 注意：comps 注册序与启动序相等由 startProcessor 的"先 ctx.add 后 start"结构保证。 */
    private static final Map<Class<?>, Integer> VENDOR_TIMELINE_ORDINAL;

    static {
        Map<Class<?>, Integer> m = new HashMap<>();

        m.put(PoolProcessor.class, 8);
        m.put(GridTimeoutProcessor.class, 12);

        VENDOR_TIMELINE_ORDINAL = m;
    }

    /** */
    @After public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * @return 全默认测试配置（静音日志）。
     */
    private IgniteConfiguration config() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(NAME);
        cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    /**
     * tracer：Ignition.start() 返回且工厂级状态 STARTED，kernal 级 gateway 亦 STARTED。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testIgnitionStartReturnsStarted() throws Exception {
        Ignite ignite = Ignition.start(config());

        assertNotNull(ignite);

        assertEquals(IgniteState.STARTED, Ignition.state(NAME));

        assertTrue("IgniteKernal must implement IgniteEx", ignite instanceof IgniteEx);

        IgniteEx ex = (IgniteEx)ignite;

        assertNotNull(ex.context());

        assertEquals(NAME, ex.context().igniteInstanceName());

        assertNotNull("localNodeId must come from kernal context", ((IgniteKernal)ex).localNodeId());

        assertEquals(GridKernalState.STARTED, ex.context().gateway().getState());
    }

    /**
     * 生长不变量：components() 注册序必须按 vendor 时间线序严格递增（子序列）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testStartupSequenceIsVendorTimelineSubsequence() throws Exception {
        IgniteEx ex = (IgniteEx)Ignition.start(config());

        int lastOrd = -1;

        for (GridComponent comp : ex.context().components()) {
            Integer ord = VENDOR_TIMELINE_ORDINAL.get(comp.getClass());

            assertNotNull("组件未登记 vendor 时间线序号（生长不变量要求登记）: "
                + comp.getClass().getName(), ord);

            assertTrue("启动序列违反 vendor 时间线子序列不变量: "
                + comp.getClass().getSimpleName() + " (ord=" + ord + ") after ord=" + lastOrd,
                ord > lastOrd);

            lastOrd = ord;
        }

        // 0.3 最小闭包：PoolProcessor(P8) + GridTimeoutProcessor(P12)。
        assertEquals(2, ex.context().components().size());
    }

    /**
     * 最小闭包三件套就位：marshaller 绑定 context、pool 可执行、timeout 可调度。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testMinimalClosureWired() throws Exception {
        IgniteEx ex = (IgniteEx)Ignition.start(config());

        GridKernalContext ctx = ex.context();

        // marshaller：BinaryMarshaller（delegate 占位）已绑定 marshaller context 与实例名。
        assertNotNull(ctx.marshaller());

        assertNotNull(ctx.marshallerContext());

        assertSame(ctx.marshallerContext(), ctx.marshaller().getContext());

        // JdkMarshaller 往返（tracer 第二肢）。
        byte[] bytes = ctx.marshaller().marshal("roundtrip");

        assertEquals("roundtrip", ctx.marshaller().unmarshal(bytes, null));

        // pool：公共池可执行任务。
        Object res = ctx.pool().getExecutorService().submit(() -> "pool-ok").get();

        assertEquals("pool-ok", res);

        // timeout：可调度一次性任务。
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        ctx.timeout().addTimeoutObject(new org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter(50) {
            @Override public void onTimeout() {
                latch.countDown();
            }
        });

        assertTrue("timeout callback must fire", latch.await(5, java.util.concurrent.TimeUnit.SECONDS));
    }

    /**
     * 失败回滚：组件启动失败后 gateway 复位 STOPPED、注册表摘除、同名可重启。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testStartFailureRollsBack() throws Exception {
        IgniteConfiguration bad = config();

        bad.setPublicThreadPoolSize(0); // PoolProcessor.start 校验失败。

        try {
            Ignition.start(bad);

            fail("Start must fail with invalid pool size");
        }
        catch (org.apache.ignite.IgniteException e) {
            assertTrue("Failure must mention public pool: " + e, chainContains(e, "public"));
        }

        assertEquals(IgniteState.STOPPED, Ignition.state(NAME));

        assertFalse(Ignition.stop(NAME, true));

        // 同名重启成功——回滚干净（含失败组件的半启动状态清理）。
        Ignite ignite = Ignition.start(config());

        assertEquals(IgniteState.STARTED, Ignition.state(NAME));

        assertEquals(GridKernalState.STARTED, ((IgniteEx)ignite).context().gateway().getState());
    }

    /**
     * 正式停止走最小停链：组件逆序 stop（后台 worker 不泄漏）、gateway 复位 STOPPED。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testStopResetsGatewayAndStopsComponents() throws Exception {
        IgniteEx ex = (IgniteEx)Ignition.start(config());

        GridKernalGateway gw = ex.context().gateway();

        assertEquals(GridKernalState.STARTED, gw.getState());

        assertTrue(Ignition.stop(NAME, true));

        assertEquals(GridKernalState.STOPPED, gw.getState());

        // 停链完成后 kernal 上下文摘除（组件已逆序 stop，worker 线程已退出）。
        assertNull(ex.context());

        // 停止后工厂状态 STOPPED，且再次 stop 返回 false。
        assertEquals(IgniteState.STOPPED, Ignition.state(NAME));

        assertFalse(Ignition.stop(NAME, true));
    }

    /**
     * 沿 cause 链查找包含指定子串的消息。
     *
     * @param e 异常。
     * @param sub 子串。
     * @return 命中返回 {@code true}。
     */
    private static boolean chainContains(Throwable e, String sub) {
        for (Throwable t = e; t != null; t = t.getCause())
            if (t.getMessage() != null && t.getMessage().contains(sub))
                return true;

        return false;
    }
}
