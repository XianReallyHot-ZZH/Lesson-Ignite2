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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/typedef/X.java
// Lesson 0.1 子集：仅 hasCause/cause 两个方法 + 私有遍历器（供 IgniteCheckedException 委托）。
// 遍历顺序与 vendor searchForCause 一致：自身 → cause 链（深度优先）→ suppressed，IdentityHashMap 防环。
// vendor X 是 typedef 大杂烩（F.isEmpty、异常渲染等），其余函数随课生长。

package org.apache.ignite.internal.util.typedef;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

/**
 * Defines global scope.
 * <p>
 * Contains often used utility functions allowing to cut down on code bloat. Note that this
 * should only be used for static constants and static utility functions.
 */
public final class X {
    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy <b>including</b> that
     * throwable itself.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes, {@code false}
     * otherwise.
     */
    public static boolean hasCause(@Nullable Throwable t, @Nullable Class<?>... cls) {
        if (t == null || cls == null || cls.length == 0)
            return false;

        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());

        return searchForCause(t, dejaVu, cls) != null;
    }

    /**
     * Gets first exception of given class from {@code 'cause'} hierarchy if any.
     *
     * @param t Throwable to check (if {@code null}, {@code null} is returned).
     * @param cls Cause class to get cause (if {@code null}, {@code null} is returned).
     * @param <T> Type of the exception cause.
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    @Nullable public static <T extends Throwable> T cause(@Nullable Throwable t, @Nullable Class<T> cls) {
        if (t == null || cls == null)
            return null;

        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());

        @SuppressWarnings("unchecked")
        T res = (T)searchForCause(t, dejaVu, cls);

        return res;
    }

    /**
     * Traverses tree of {@link Throwable} to find first node assignable from any of given types.
     * Function is aware of possible circular references through tracking of tested objects.
     *
     * @param t Throwable.
     * @param dejaVu Set of throwable for tracking already tested objects.
     * @param types Candidate types.
     * @return First throwable meets test condition or {@code null} if none has matched.
     */
    @Nullable private static Throwable searchForCause(Throwable t, Set<Throwable> dejaVu, Class<?>... types) {
        for (Class<?> c : types)
            if (c != null && c.isAssignableFrom(t.getClass()))
                return t;

        if (!dejaVu.add(t))
            return null;

        Throwable cause = t.getCause();

        if (cause != null) {
            Throwable found = searchForCause(cause, dejaVu, types);

            if (found != null)
                return found;
        }

        for (Throwable suppressed : t.getSuppressed()) {
            Throwable found = searchForCause(suppressed, dejaVu, types);

            if (found != null)
                return found;
        }

        return null;
    }
}
