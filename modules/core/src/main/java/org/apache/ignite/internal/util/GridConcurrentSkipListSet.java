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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/util/GridConcurrentSkipListSet.java
// Lesson 0.3 子集：firstx()（取首元素或 null）+ 集合透传。vendor 另有
// firstx/lastx/ceilingx 等取值族与 tailSet/headSet 快照——随消费者出现补。

package org.apache.ignite.internal.util;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import org.jetbrains.annotations.Nullable;

/**
 * 并发有序集合包装（在 ConcurrentSkipListSet 上补"或 null 取值"访问器）。
 *
 * @param <E> 元素类型。
 */
public class GridConcurrentSkipListSet<E> extends AbstractSet<E> {
    /** 底层集合。 */
    private final ConcurrentSkipListSet<E> set;

    /**
     * @param cmp 比较器（可为 {@code null}——自然序）。
     */
    public GridConcurrentSkipListSet(@Nullable Comparator<? super E> cmp) {
        set = cmp != null ? new ConcurrentSkipListSet<>(cmp) : new ConcurrentSkipListSet<>();
    }

    /**
     * @return 首元素（空集返回 {@code null}——原生 first() 会抛异常）。
     */
    @Nullable public E firstx() {
        return set.isEmpty() ? null : set.first();
    }

    /**
     * @return 末元素（空集返回 {@code null}）。
     */
    @Nullable public E lastx() {
        return set.isEmpty() ? null : set.last();
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return set.iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return set.size();
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        return set.add(e);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return set.remove(o);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return set.contains(o);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * @return 底层 NavigableSet 视图（区间操作随消费者出现使用）。
     */
    public NavigableSet<E> set() {
        return set;
    }
}
