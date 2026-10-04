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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/IgniteCheckedException.java
// Lesson 0.1 子集：完整复刻（构造器五件套 + hasCause/getCause(Class) + toString）。
// 这是复刻的第一个类——内核里几乎所有 checked 异常的基类。

package org.apache.ignite;

import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 * General grid exception. This exception is used to indicate any error condition
 * within Grid.
 */
public class IgniteCheckedException extends Exception {
    // 【教学】Ignite 的异常体系是"成对"的：本类是 checked 版，用于内核内部路径，
    // 强制调用方显式处理；其 unchecked 兄弟 IgniteException 留给公共 API 层（vendor 同在
    // commons，随 0.2 课需要引入）。0.2 课的 Ignition.start(cfg) 签名将 throws 本类——
    // 它是后续所有课程错误路径的通用语言。

    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public IgniteCheckedException() {
        // No-op.
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteCheckedException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteCheckedException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    public IgniteCheckedException(String msg, @Nullable Throwable cause, boolean writableStackTrace) {
        super(msg, cause, true, writableStackTrace);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCheckedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Checks if this exception has given class in {@code 'cause'} hierarchy.
     *
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    // 【教学】hasCause/getCause(Class) 是内核里最高频的异常查询面：拿到一个深层包装的
    // 异常后问"里面到底有没有某类错误"。遍历序（自身→cause 链→suppressed）与防环语义
    // 都在 X.searchForCause，测试已逐条锁定。

    @SafeVarargs
    public final boolean hasCause(@Nullable Class<? extends Throwable>... cls) {
        return X.hasCause(this, cls);
    }

    /**
     * Gets first exception of given class from {@code 'cause'} hierarchy if any.
     *
     * @param cls Cause class to get cause (if {@code null}, {@code null} is returned).
     * @param <T> Type of the exception cause.
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    @Nullable public <T extends Throwable> T getCause(@Nullable Class<T> cls) {
        return X.cause(this, cls);
    }

    // 【教学】vendor 刻意输出 "class 类名: 消息"（注意带 class 前缀）——
    // 错误日志里据此快速识别异常类型，测试锁定了该格式。

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
