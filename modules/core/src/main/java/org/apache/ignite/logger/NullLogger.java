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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/logger/NullLogger.java
// Lesson 0.2 子集：完整复刻（全部方法丢弃日志；INSTANCE 单例 + whenNull 助手一并保留）。

package org.apache.ignite.logger;

import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * 丢弃所有日志的空 logger。测试静音与"无日志"配置使用。
 */
public class NullLogger implements IgniteLogger {
    /** 单例实例。 */
    public static final NullLogger INSTANCE = new NullLogger();

    /**
     * 把可能为 {@code null} 的 logger 归一化为 {@code NullLogger}。
     *
     * @param log 输入 logger。
     * @return {@code log} 本身；为 {@code null} 时返回 {@link #INSTANCE}。
     */
    public static IgniteLogger whenNull(IgniteLogger log) {
        return log == null ? INSTANCE : log;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        // 空实现。
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "NullLogger []";
    }
}
