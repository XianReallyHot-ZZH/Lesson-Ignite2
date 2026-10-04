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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/logger/java/JavaLogger.java
// Lesson 0.2 子集：JUL 门面（构造器两件套 + 静默模式 + console handler 治理 + 六级日志）。
// 未带：JavaLoggerFileHandler（文件日志，随工作目录日志课）、IgniteLoggerEx 接口面
// （setApplicationAndNode/flush/addConsoleAppender）、默认配置文件加载（仓库无该资源）。
// 静默语义与 vendor 一致：IGNITE_QUIET（默认 true）时 console 只出 SEVERE，其余级别走 JUL 配置。

package org.apache.ignite.logger.java;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * 基于 {@code java.util.logging}（JUL）的默认 logger 实现。
 * <p>
 * 用户未配置 logger 时（{@code U.initLogger} 路径），复刻与 vendor 一样落到本实现。
 */
public class JavaLogger implements IgniteLogger {
    /** 默认 JUL 配置文件路径（相对 Ignite home）。 */
    public static final String DFLT_CONFIG_PATH = "config/java.util.logging.properties";

    /** 初始化互斥。 */
    private static final Object mux = new Object();

    /** JVM 级只初始化一次标志。 */
    private static volatile boolean inited;

    /** JVM 级静默标志。 */
    private static volatile boolean quiet0;

    /** JUL 实现代理。 */
    private final Logger impl;

    /** 静默标志。 */
    private final boolean quiet;

    /** 工作目录（文件 handler 接入后使用；当前仅保存）。 */
    private volatile String workDir;

    /**
     * 以默认方式创建 logger（等价 {@code JavaLogger(true)}）。
     */
    public JavaLogger() {
        this(true);
    }

    /**
     * 是否已由外部（如容器/脚本）提供 JUL 配置。
     *
     * @return 已配置返回 {@code true}。
     */
    public static boolean isConfigured() {
        return System.getProperty("java.util.logging.config.file") != null;
    }

    /**
     * 创建 logger。
     *
     * @param init 是否执行 JUL console 治理（单测里多次构造时传 {@code false} 可跳过）。
     */
    public JavaLogger(boolean init) {
        impl = Logger.getLogger("");

        if (init) {
            configure();

            quiet = quiet0;
        }
        else
            quiet = true;
    }

    /**
     * 以给定 JUL logger 创建 logger（getLogger 派生类别用）。
     *
     * @param impl JUL logger。
     */
    private JavaLogger(final Logger impl) {
        this.impl = impl;

        quiet = quiet0;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new JavaLogger(ctgr == null
            ? Logger.getLogger("")
            : Logger.getLogger(ctgr instanceof Class
                ? ((Class<?>)ctgr).getName()
                : String.valueOf(ctgr)));
    }

    /**
     * 治理 console handler：静默模式下把 console 级别压到 SEVERE。
     * 与 vendor 逻辑对齐（去掉 vendor 的默认配置文件加载段——复刻仓库无该资源）。
     */
    private void configure() {
        if (inited)
            return;

        synchronized (mux) {
            if (inited)
                return;

            if (isConfigured()) {
                // 用户自带 JUL 配置且配置了 console handler，视为非静默。
                boolean consoleHndFound = findHandler(impl, ConsoleHandler.class) != null;

                quiet0 = !consoleHndFound;
                inited = true;

                return;
            }

            boolean quiet = getBoolean(IgniteSystemProperties.IGNITE_QUIET, true);

            ConsoleHandler consoleHnd = (ConsoleHandler)findHandler(impl, ConsoleHandler.class);

            if (consoleHnd != null)
                consoleHnd.setLevel(quiet ? Level.SEVERE : Level.INFO);
            else
                // JUL 默认根 logger 就有一个 ConsoleHandler；走到这里说明被外部清过，
                // 与 vendor 一致只提示不重建。
                System.err.println("Console logging handler is not configured.");

            quiet0 = quiet;
            inited = true;
        }
    }

    /**
     * 在 logger 层级里找指定类型的 handler。
     *
     * @param lgr 起点 logger。
     * @param hndCls handler 类型。
     * @return 找到的 handler 或 {@code null}。
     */
    @Nullable private static Handler findHandler(Logger lgr, Class<? extends Handler> hndCls) {
        for (Handler h : lgr.getHandlers())
            if (hndCls.isAssignableFrom(h.getClass()))
                return h;

        return lgr.getParent() == null ? null : findHandler(lgr.getParent(), hndCls);
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        impl.finest(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        impl.fine(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        impl.info(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warning(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        impl.log(Level.WARNING, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.severe(msg);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.log(Level.SEVERE, msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isLoggable(Level.FINEST);
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isLoggable(Level.FINE);
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isLoggable(Level.INFO);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return quiet;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        // vendor：从 JavaLoggerFileHandler/FileHandler 取当前日志文件；文件 handler 未复刻，恒 null。
        return null;
    }

    /**
     * 设置工作目录（{@code U.initLogger} 在无用户 logger 时调用）。
     *
     * @param workDir 工作目录。
     */
    public void setWorkDirectory(String workDir) {
        this.workDir = workDir;
    }
}
