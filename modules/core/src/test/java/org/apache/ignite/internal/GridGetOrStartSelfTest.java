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

/*
 * 改写自 Apache Ignite 2.18.0 官方测试（Apache License 2.0，保留上方原始版权头）：
 * vendors/ignite/modules/core/src/test/java/org/apache/ignite/internal/GridGetOrStartSelfTest.java
 * （IGNITE-2941 的 getOrStart 语义测试）。
 *
 * 适配点（复刻边界内保持断言语义不变）：
 * - 不依赖 vendor 测试框架 GridCommonAbstractTest / @GridCommonTest 注解——
 *   以纯 JUnit 4 + 本地 getConfiguration(name) 助手 + @After stopAll 重建同等隔离；
 * - vendor 的 getConfiguration 会注入测试框架的 GridTestNodeLog 等，这里以 NullLogger 静音代替；
 * - 断言原文保留：getOrStart 幂等返回同一实例、二次 start 抛 IgniteException。
 */

package org.apache.ignite.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.NullLogger;
import org.junit.After;
import org.junit.Test;

/**
 * GridGetOrStartSelfTest 测试 get-or-start 语义。参见 IGNITE-2941。
 */
public class GridGetOrStartSelfTest {
    /**
     * 默认构造器。
     */
    public GridGetOrStartSelfTest() {
        // 无额外初始化。
    }

    /**
     * 每个测试后停掉 JVM 内全部实例，隔离静态注册表。
     */
    @After
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * 测试默认（无名）Ignite 实例的 getOrStart。
     */
    @Test
    public void testDefaultIgniteInstanceGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration(null);

        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
                // 预期路径：二次 start 已启动实例 → IgniteException。
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }
    }

    /**
     * 测试命名 Ignite 实例的 getOrStart。
     */
    @Test
    public void testNamedIgniteInstanceGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test");

        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
                // 预期路径：二次 start 已启动实例 → IgniteException。
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }
    }

    /**
     * 构造测试配置（镜像 vendor GridCommonAbstractTest.getConfiguration 的最小形态）。
     *
     * @param name 实例名（可为 {@code null} 表示默认实例）。
     * @return 配置。
     */
    private IgniteConfiguration getConfiguration(String name) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setGridLogger(new NullLogger());

        return cfg;
    }
}
