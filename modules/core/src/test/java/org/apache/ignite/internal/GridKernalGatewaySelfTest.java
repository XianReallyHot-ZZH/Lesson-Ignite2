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

// Lesson 0.3 自写：gateway 状态机切片（course-map §0.3 概念簇第一肢）。
// 语义对齐 vendor GridKernalGatewayImpl（读写锁 + 状态驱动的公共 API 准入）。

package org.apache.ignite.internal;

import org.apache.ignite.IgniteIllegalStateException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * GridKernalGatewayImpl 状态机测试。
 */
public class GridKernalGatewaySelfTest {
    /**
     * 初始状态必为 STOPPED（vendor 同款初始态）。
     */
    @Test
    public void testInitialStateIsStopped() {
        assertEquals(GridKernalState.STOPPED, new GridKernalGatewayImpl("gw-test").getState());
    }

    /**
     * 非 STARTED 状态下 readLock 必须拒绝（IgniteIllegalStateException），
     * 且不给调用者留下未释放的读锁。
     */
    @Test
    public void testReadLockRejectedBeforeStarted() {
        GridKernalGatewayImpl gw = new GridKernalGatewayImpl("gw-test");

        gw.writeLock();

        try {
            gw.setState(GridKernalState.STARTING);
        }
        finally {
            gw.writeUnlock();
        }

        try {
            gw.readLock();

            fail("readLock must reject when grid is starting");
        }
        catch (IgniteIllegalStateException e) {
            assertNotNull(e.getMessage());
        }

        // STARTING 下被拒后，readLockAnyway 仍可进入（stop 路径需要它）。
        gw.readLockAnyway();

        gw.readUnlock();
    }

    /**
     * STARTED 后 readLock/readUnlock 畅通，userStackTrace 在首次进入后可用。
     */
    @Test
    public void testReadLockAcceptedWhenStarted() {
        GridKernalGatewayImpl gw = new GridKernalGatewayImpl("gw-test");

        gw.writeLock();

        try {
            gw.setState(GridKernalState.STARTED);
        }
        finally {
            gw.writeUnlock();
        }

        gw.readLock();

        assertEquals(GridKernalState.STARTED, gw.getState());

        assertNotNull("user stack trace must be captured on first entry", gw.userStackTrace());

        gw.readUnlock();

        assertNotNull(gw.userStackTrace());
    }

    /**
     * setState 必须在写锁内调用是 vendor 约定；这里验证状态往返
     * STOPPED→STARTING→STARTED→STOPPING→STOPPED 全枚举可达。
     */
    @Test
    public void testFullStateCycle() {
        GridKernalGatewayImpl gw = new GridKernalGatewayImpl("gw-test");

        for (GridKernalState state : new GridKernalState[] {
            GridKernalState.STARTING, GridKernalState.STARTED,
            GridKernalState.STOPPING, GridKernalState.STOPPED}) {
            gw.writeLock();

            try {
                gw.setState(state);
            }
            finally {
                gw.writeUnlock();
            }

            assertEquals(state, gw.getState());
        }
    }

    /**
     * 写锁独占：被别线程持有时 tryWriteLock 超时返回 false，释放后可获取。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testTryWriteLockExclusion() throws Exception {
        final GridKernalGatewayImpl gw = new GridKernalGatewayImpl("gw-test");

        Thread holder = new Thread(() -> {
            gw.writeLock();

            try {
                try {
                    Thread.sleep(300);
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }
            finally {
                gw.writeUnlock();
            }
        });

        holder.start();

        // 等 holder 拿到写锁。
        Thread.sleep(100);

        assertFalse(gw.tryWriteLock(50));

        holder.join();

        assertTrue(gw.tryWriteLock(1000));

        gw.writeUnlock();
    }
}
