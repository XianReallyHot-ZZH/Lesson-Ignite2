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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/IgniteInterruptedCheckedException.java
// Lesson 0.2 子集：完整复刻（三个构造器即 vendor 全部成员）。
// IgnitionEx.start0 在启动线程被中断时用它做 cause 链识别（X.hasCause）。

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;

/**
 * 可中断操作被中断时抛出的 checked 异常。
 * <p>
 * 调用方捕获本异常后应恢复 {@code Thread.currentThread().interrupt()} 标志位。
 */
public class IgniteInterruptedCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 以中断异常为 cause 创建新异常。
     *
     * @param cause 中断 cause。
     */
    public IgniteInterruptedCheckedException(InterruptedException cause) {
        super(cause);
    }

    /**
     * 以给定错误消息创建新异常。
     *
     * @param msg 错误消息。
     */
    public IgniteInterruptedCheckedException(String msg) {
        super(msg);
    }

    /**
     * 以给定错误消息与中断 cause 创建新异常。
     *
     * @param msg 错误消息。
     * @param cause 中断 cause。
     */
    public IgniteInterruptedCheckedException(String msg, InterruptedException cause) {
        super(msg, cause);
    }
}
