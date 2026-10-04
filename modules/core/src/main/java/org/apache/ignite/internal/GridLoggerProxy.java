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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridLoggerProxy.java
// Lesson 0.2 子集：日志代理本体（getLogger 派生 + 实例名/节点 id8 富化 + LifecycleAware 透传）。
// 未带：Externalizable 序列化三件套（writeExternal/readExternal/readResolve，
// 依赖 marshaller 与 IgnitionEx.localIgnite()，随章 3 marshaller 课接入）。
// toString 手写（vendor 用 typedef.internal.S 反射拼装，S 随课生长后再换）。

package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_GRID_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_INSTANCE_NAME;

/**
 * Grid logger 代理。给任意 logger 实现附上实例名与节点 id8 上下文，
 * 多实例同 JVM 时区分日志归属。
 */
public class GridLoggerProxy implements IgniteLogger, LifecycleAware {
    /** 被代理的 logger 实现。 */
    private final IgniteLogger impl;

    /** logger 类别（getLogger 派生时记录）。 */
    private final Object ctgr;

    /** Ignite 实例名（默认实例为 {@code null}）。 */
    private final String igniteInstanceName;

    /** 节点 ID 的 8 位短表示。 */
    private final String id8;

    /** 是否在日志消息中附带实例名。 */
    private static final boolean logIgniteInstanceName = System.getProperty(IGNITE_LOG_INSTANCE_NAME) != null ||
        System.getProperty(IGNITE_LOG_GRID_NAME) != null;

    /**
     * 创建代理。
     *
     * @param impl 被代理的 logger 实现。
     * @param ctgr 可选 logger 类别。
     * @param igniteInstanceName Ignite 实例名（默认实例可为 {@code null}）。
     * @param id8 节点 ID 短表示。
     */
    public GridLoggerProxy(IgniteLogger impl, @Nullable Object ctgr, @Nullable String igniteInstanceName,
        String id8) {
        assert impl != null;

        this.impl = impl;
        this.ctgr = ctgr;
        this.igniteInstanceName = igniteInstanceName;
        this.id8 = id8;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (impl instanceof LifecycleAware)
            ((LifecycleAware)impl).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // vendor 走 U.stopLifecycleAware(this, singleton(impl))——给被停组件记日志，
        // 日志治理课接入后对齐；当前直接透传。
        if (impl instanceof LifecycleAware)
            ((LifecycleAware)impl).stop();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        assert ctgr != null;

        return new GridLoggerProxy(impl.getLogger(ctgr), ctgr, igniteInstanceName, id8);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return impl.fileName();
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        impl.trace(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void trace(@Nullable String marker, String msg) {
        impl.trace(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        impl.debug(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(@Nullable String marker, String msg) {
        impl.debug(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        impl.info(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(@Nullable String marker, String msg) {
        impl.info(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warning(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, Throwable e) {
        impl.warning(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void warning(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.warning(marker, enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.error(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, Throwable e) {
        impl.error(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.error(marker, enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return impl.isQuiet();
    }

    /**
     * 取被代理实现的描述信息（实现类名与参数）。
     *
     * @return logger 实现信息。
     */
    public String getLoggerInfo() {
        return impl.toString();
    }

    /**
     * 设置了 {@code IGNITE_LOG_INSTANCE_NAME}（或旧名 {@code IGNITE_LOG_GRID_NAME}）时，
     * 在消息前附加 {@code <实例名-id8>} 前缀。
     *
     * @param m 原消息。
     * @return 富化后的消息或原消息。
     */
    private String enrich(@Nullable String m) {
        return logIgniteInstanceName && m != null ? "<" + igniteInstanceName + '-' + id8 + "> " + m : m;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridLoggerProxy [igniteInstanceName=" + igniteInstanceName
            + ", id8=" + id8
            + ", ctgr=" + ctgr + ']';
    }
}
