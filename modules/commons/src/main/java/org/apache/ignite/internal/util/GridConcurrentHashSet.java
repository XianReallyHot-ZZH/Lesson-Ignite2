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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/GridConcurrentHashSet.java
// Lesson 0.2 子集：并发 Set 语义（add/remove/contains/iterator/size）。
// vendor 版本 extends GridSetWrapper 并多四个构造器重载；复刻直接组合 ConcurrentHashMap 键视图，
// 迭代语义（弱一致、不抛 ConcurrentModificationException）与 vendor 一致。

package org.apache.ignite.internal.util;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 {@link ConcurrentHashMap} 的线程安全 Set 实现。
 */
public class GridConcurrentHashSet<E> extends AbstractSet<E> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** 底层存储：键为元素、值为常量哨兵。 */
    private final ConcurrentHashMap<E, Boolean> map;

    /**
     * 以默认初始容量创建集合。
     */
    public GridConcurrentHashSet() {
        this(16);
    }

    /**
     * 以给定初始容量创建集合。
     *
     * @param initCap 初始容量。
     */
    public GridConcurrentHashSet(int initCap) {
        map = new ConcurrentHashMap<>(initCap);
    }

    /**
     * 以给定元素集合创建集合。
     *
     * @param c 初始元素。
     */
    public GridConcurrentHashSet(Collection<E> c) {
        map = new ConcurrentHashMap<>(c.size());

        addAll(c);
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        return map.put(e, Boolean.TRUE) == null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }
}
