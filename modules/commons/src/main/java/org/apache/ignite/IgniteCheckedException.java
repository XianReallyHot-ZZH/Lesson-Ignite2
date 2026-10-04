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
 * Grid 通用异常。用于指示 Grid 内部的任何错误状态。
 */
public class IgniteCheckedException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 创建空异常。
     */
    public IgniteCheckedException() {
        // 空实现。
    }

    /**
     * 以给定错误消息创建新异常。
     *
     * @param msg 错误消息。
     */
    public IgniteCheckedException(String msg) {
        super(msg);
    }

    /**
     * 以给定 Throwable 作为 cause 及错误消息来源创建新 grid 异常。
     *
     * @param cause 非空 Throwable cause。
     */
    public IgniteCheckedException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * 以给定错误消息与可选嵌套异常创建新异常。
     *
     * @param msg 错误消息。
     * @param cause 可选嵌套异常（可为 {@code null}）。
     * @param writableStackTrace 堆栈是否可写。
     */
    public IgniteCheckedException(String msg, @Nullable Throwable cause, boolean writableStackTrace) {
        super(msg, cause, true, writableStackTrace);
    }

    /**
     * 以给定错误消息与可选嵌套异常创建新异常。
     *
     * @param msg 错误消息。
     * @param cause 可选嵌套异常（可为 {@code null}）。
     */
    public IgniteCheckedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * 检查本异常的 {@code 'cause'} 层级中是否含给定类。
     *
     * @param cls 待检查的 cause 类（若为 {@code null} 或空，返回 {@code false}）。
     * @return 若某个 cause 异常是传入类的实例则返回 {@code true}，否则 {@code false}。
     */
    @SafeVarargs
    public final boolean hasCause(@Nullable Class<? extends Throwable>... cls) {
        return X.hasCause(this, cls);
    }

    /**
     * 从 {@code 'cause'} 层级中取得第一个给定类的异常（若有）。
     *
     * @param cls 要获取的 cause 类（若为 {@code null}，返回 {@code null}）。
     * @param <T> 异常 cause 的类型。
     * @return 传入类的第一个 cause 异常，否则 {@code null}。
     */
    @Nullable public <T extends Throwable> T getCause(@Nullable Class<T> cls) {
        return X.cause(this, cls);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
