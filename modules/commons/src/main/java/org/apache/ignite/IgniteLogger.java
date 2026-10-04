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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/IgniteLogger.java
// Lesson 0.2 子集：完整接口签名（getLogger + 六级日志 + marker 默认方法 + 探询方法 + fileName）。
// vendor 另有 @GridToStringExclude 类注解与 log4j2/log4j/jcl/sl4j/quiet 各实现（core 模块），
// 复刻只带 core 的 JavaLogger 与 NullLogger，其余日志实现属外围依赖不在复刻范围。

package org.apache.ignite;

import org.jetbrains.annotations.Nullable;

/**
 * 日志抽象。为兼容各类底层日志实现而设的简单接口，支持静默模式与按类别创建子 logger。
 * <p>
 * 静默模式下仅输出 ERROR 级与告警；非静默输出 INFO 及以上。
 */
public interface IgniteLogger {
    /**
     * 仅在开发环境有用、生产环境应避免的日志消息 marker。
     */
    String DEV_ONLY = "DEV_ONLY";

    /**
     * 以给定类别基于当前实例创建新 logger。
     *
     * @param ctgr 新 logger 的类别。
     * @return 给定类别的新 logger。
     */
    IgniteLogger getLogger(Object ctgr);

    /**
     * 输出 trace 消息。
     *
     * @param msg trace 消息。
     */
    void trace(String msg);

    /**
     * 输出带 marker 的 trace 消息。
     * <p>
     * 默认实现直接调用 {@code this.trace(msg)}。
     *
     * @param marker 与消息关联的 marker 名。
     * @param msg trace 消息。
     */
    default void trace(@Nullable String marker, String msg) {
        trace(msg);
    }

    /**
     * 输出 debug 消息。
     *
     * @param msg debug 消息。
     */
    void debug(String msg);

    /**
     * 输出带 marker 的 debug 消息。
     * <p>
     * 默认实现直接调用 {@code this.debug(msg)}。
     *
     * @param marker 与消息关联的 marker 名。
     * @param msg debug 消息。
     */
    default void debug(@Nullable String marker, String msg) {
        debug(msg);
    }

    /**
     * 输出 info 消息。
     *
     * @param msg info 消息。
     */
    void info(String msg);

    /**
     * 输出带 marker 的 info 消息。
     * <p>
     * 默认实现直接调用 {@code this.info(msg)}。
     *
     * @param marker 与消息关联的 marker 名。
     * @param msg info 消息。
     */
    default void info(@Nullable String marker, String msg) {
        info(msg);
    }

    /**
     * 输出告警消息。
     *
     * @param msg 告警消息。
     */
    void warning(String msg);

    /**
     * 输出带异常的告警消息。
     *
     * @param msg 告警消息。
     * @param e 输出异常（可为 {@code null}）。
     */
    void warning(String msg, @Nullable Throwable e);

    /**
     * 输出带 marker 与异常的告警消息。
     * <p>
     * 默认实现直接调用 {@code this.warning(msg, e)}。
     *
     * @param marker 与消息关联的 marker 名。
     * @param msg 告警消息。
     * @param e 输出异常（可为 {@code null}）。
     */
    default void warning(@Nullable String marker, String msg, @Nullable Throwable e) {
        warning(msg, e);
    }

    /**
     * 输出错误消息。
     *
     * @param msg 错误消息。
     */
    void error(String msg);

    /**
     * 输出带异常的错误消息。
     *
     * @param msg 错误消息。
     * @param e 输出异常（可为 {@code null}）。
     */
    void error(String msg, @Nullable Throwable e);

    /**
     * 输出带 marker 与异常的错误消息。
     * <p>
     * 默认实现直接调用 {@code this.error(msg, e)}。
     *
     * @param marker 与消息关联的 marker 名。
     * @param msg 错误消息。
     * @param e 输出异常（可为 {@code null}）。
     */
    default void error(@Nullable String marker, String msg, @Nullable Throwable e) {
        error(msg, e);
    }

    /**
     * 测试 trace 级是否启用。
     *
     * @return 若启用 trace 则返回 {@code true}。
     */
    boolean isTraceEnabled();

    /**
     * 测试 debug 级是否启用。
     *
     * @return 若启用 debug 则返回 {@code true}。
     */
    boolean isDebugEnabled();

    /**
     * 测试 info 级是否启用。
     *
     * @return 若启用 info 则返回 {@code true}。
     */
    boolean isInfoEnabled();

    /**
     * 测试本 logger 是否处于静默模式。
     *
     * @return 若处于静默模式则返回 {@code true}。
     */
    boolean isQuiet();

    /**
     * 取得底层日志实现当前写入的文件名（若可用）。
     *
     * @return 文件名，或不可用时的 {@code null}。
     */
    @Nullable String fileName();
}
