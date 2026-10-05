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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/lang/GridAbsClosure.java
// Lesson 0.3 子集：完整类（无参无返回值的绝对闭包）。
// 消费者：IgniteKernal.start 全签名的 errHnd 参数（启动失败回调）。

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.lang.IgniteRunnable;

/**
 * 定义一个便捷的"绝对"闭包：无入参、无返回值（{@code void}）。
 * <p>
 * 线程安全性：本接口不强制也不假设任何线程安全语义，由实现自行决定。
 */
public abstract class GridAbsClosure implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 闭包体。
     */
    public abstract void apply();

    /**
     * 委托到 {@link #apply()}。
     */
    @Override public final void run() {
        apply();
    }
}
