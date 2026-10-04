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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/IgniteIllegalStateException.java
// Lesson 0.2 子集：完整复刻（构造器三件套 + hasCause；vendor 即此成员集）。
// Ignition.ignite(name) 在实例不存在时抛出（IllegalStateException 语义：非法状态下访问）。

package org.apache.ignite;

import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 * 本异常指示在非法状态下访问 Ignite（如实例未启动就被引用）。
 */
public class IgniteIllegalStateException extends IllegalStateException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 以给定消息与 cause 构造异常。
     *
     * @param msg 异常消息。
     * @param cause cause（可为 {@code null}）。
     */
    public IgniteIllegalStateException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * 以给定 Throwable 作为 cause 及错误消息来源构造异常。
     *
     * @param cause 非空 Throwable cause。
     */
    public IgniteIllegalStateException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * 以给定消息构造异常。
     *
     * @param msg 异常消息。
     */
    public IgniteIllegalStateException(String msg) {
        super(msg);
    }

    /**
     * 检查本异常的 {@code 'cause'} 层级中是否含给定类。
     *
     * @param cls 待检查的 cause 类（若为 {@code null}，返回 {@code false}）。
     * @return 若某个 cause 异常是传入类的实例则返回 {@code true}，否则 {@code false}。
     */
    public boolean hasCause(@Nullable Class<? extends Throwable> cls) {
        return X.hasCause(this, cls);
    }
}
