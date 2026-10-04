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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/lang/IgniteBiTuple.java
// Lesson 0.2 子集：二元组核心（构造器 + get/set + equals/hashCode/toString + swap）。
// vendor 版本另实现 Map/Map.Entry/Iterable/Comparable/Cloneable/Externalizable 全家桶，
// 这些接口面随消费方出现再补（当前唯一消费者是 T2/IgnitionEx.start0 返回值）。

package org.apache.ignite.lang;

import java.io.Serializable;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * 二元组，持有一对有类型的值。
 */
public class IgniteBiTuple<V1, V2> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** 第一个值。 */
    @Nullable private V1 val1;

    /** 第二个值。 */
    @Nullable private V2 val2;

    /**
     * 创建空二元组。
     */
    public IgniteBiTuple() {
        // 空实现。
    }

    /**
     * 以给定值创建二元组。
     *
     * @param val1 第一个值。
     * @param val2 第二个值。
     */
    public IgniteBiTuple(@Nullable V1 val1, @Nullable V2 val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    /**
     * 交换两值的位置得到新二元组。
     *
     * @return 交换后的新二元组。
     */
    public IgniteBiTuple<V2, V1> swap() {
        return new IgniteBiTuple<>(val2, val1);
    }

    /**
     * 取第一个值。
     *
     * @return 第一个值。
     */
    @Nullable public V1 get1() {
        return val1;
    }

    /**
     * 取第二个值。
     *
     * @return 第二个值。
     */
    @Nullable public V2 get2() {
        return val2;
    }

    /**
     * 设置第一个值。
     *
     * @param val1 第一个值。
     */
    public void set1(@Nullable V1 val1) {
        this.val1 = val1;
    }

    /**
     * 设置第二个值。
     *
     * @param val2 第二个值。
     */
    public void set2(@Nullable V2 val2) {
        this.val2 = val2;
    }

    /**
     * 同时设置两个值。
     *
     * @param val1 第一个值。
     * @param val2 第二个值。
     */
    public void set(@Nullable V1 val1, @Nullable V2 val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteBiTuple))
            return false;

        IgniteBiTuple<?, ?> t = (IgniteBiTuple<?, ?>)o;

        return Objects.equals(val1, t.val1) && Objects.equals(val2, t.val2);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(val1, val2);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteBiTuple [val1=" + val1 + ", val2=" + val2 + ']';
    }
}
