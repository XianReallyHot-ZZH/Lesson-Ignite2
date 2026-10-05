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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/MarshallerExclusions.java
// Lesson 0.3 子集：完整类。差异：结果缓存用 ConcurrentHashMap
//（vendor 为 GridBoundedConcurrentLinkedHashMap 有界缓存，容量治理随持久化课补）。
// 排除清单的初始登记发生在 MarshallerContextImpl.initializeMarshallerExclusions（core 模块）。

package org.apache.ignite.marshaller;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 判断给定类是否应被排除出 marshalling。
 */
public final class MarshallerExclusions {
    /** 必须参与序列化的类（优先级高于 {@link #EXCL_CLASSES}）。 */
    private static final Set<Class<?>> INCL_CLASSES = new HashSet<>();

    /** 结果缓存。 */
    private static volatile Map<Class<?>, Boolean> cache = new ConcurrentHashMap<>();

    /** 应被排除的 grid 类：这些类型的字段序列化为 {@code null}。 */
    private static final Set<Class<?>> EXCL_CLASSES = new HashSet<>();

    /**
     * 单例约束。
     */
    private MarshallerExclusions() {
        // 空实现。
    }

    /**
     * 对给定类执行排除判定。
     *
     * @param cls 待检类。
     * @return 应排除返回 {@code true}。
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private static boolean isExcluded0(Class<?> cls) {
        assert cls != null;

        for (Class<?> inclCls : INCL_CLASSES)
            if (inclCls.isAssignableFrom(cls))
                return false;

        for (Class<?> exclCls : EXCL_CLASSES)
            if (exclCls.isAssignableFrom(cls))
                return true;

        return false;
    }

    /**
     * 判断给定类是否应被排除出 marshalling。
     *
     * @param cls 待检类。
     * @return 应排除返回 {@code true}。
     */
    public static boolean isExcluded(Class<?> cls) {
        Boolean res = cache.get(cls);

        if (res == null) {
            res = isExcluded0(cls);

            cache.put(cls, res);
        }

        return res;
    }

    /**
     * 清空结果缓存（仅测试用）。
     */
    public static void clearCache() {
        cache = new ConcurrentHashMap<>();
    }

    /**
     * 登记应排除的类。
     *
     * @param cls 类。
     */
    public static void exclude(Class<?> cls) {
        EXCL_CLASSES.add(cls);
    }

    /**
     * 登记必须参与序列化的类。
     *
     * @param cls 类。
     */
    public static void include(Class<?> cls) {
        INCL_CLASSES.add(cls);
    }
}
