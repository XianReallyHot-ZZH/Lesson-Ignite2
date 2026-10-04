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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/typedef/T2.java
// Lesson 0.2 子集：完整复刻（vendor 即本形态，只是基类的 Map/Entry 等接口面未带）。

package org.apache.ignite.internal.util.typedef;

import org.apache.ignite.lang.IgniteBiTuple;

/**
 * {@code typedef}：两个元素的元组。等价于 {@code IgniteBiTuple}&lt;V1, V2&gt;，
 * 仅为在 Java 缺少类型别名机制时让代码更简洁。
 */
public class T2<V1, V2> extends IgniteBiTuple<V1, V2> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * 创建空二元组。
     */
    public T2() {
        // 空实现。
    }

    /**
     * 以给定两值创建二元组。
     *
     * @param val1 第一个值。
     * @param val2 第二个值。
     */
    public T2(V1 val1, V2 val2) {
        super(val1, val2);
    }
}
