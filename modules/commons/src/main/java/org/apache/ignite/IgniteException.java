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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/IgniteException.java
// Lesson 0.2 子集：完整复刻构造器四件套（vendor 即此四个构造器，无更多成员）。
// Ignite 公共 API 的运行时异常基类；与 commons 的 IgniteCheckedException（checked）成对。

package org.apache.ignite;

import org.jetbrains.annotations.Nullable;

/**
 * Ignite 运行时异常。公共 API 面的失败以本异常（及其子类）抛出，
 * 内核内部路径则使用 checked 的 {@link IgniteCheckedException}。
 */
public class IgniteException extends RuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 创建空异常。
     */
    public IgniteException() {
        // 空实现。
    }

    /**
     * 以给定错误消息创建新异常。
     *
     * @param msg 错误消息。
     */
    public IgniteException(String msg) {
        super(msg);
    }

    /**
     * 以给定 Throwable 作为 cause 及错误消息来源创建新异常。
     *
     * @param cause 非空 Throwable cause。
     */
    public IgniteException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    /**
     * 以给定错误消息与可选嵌套异常创建新异常。
     *
     * @param msg 错误消息。
     * @param cause 可选嵌套异常（可为 {@code null}）。
     */
    public IgniteException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
