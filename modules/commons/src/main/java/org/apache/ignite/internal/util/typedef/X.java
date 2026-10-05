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
 * 定义全局作用域。
 * <p>
 * 收纳常用工具函数以削减代码膨胀。注意：只应用于静态常量与静态工具函数。
 */
public final class X {
    /**
     * 检查传入的 {@code 'Throwable'} 的 {@code 'cause'} 层级中是否含给定类，<b>包含</b>该 Throwable 自身。
     *
     * @param t 待检查的 Throwable（若为 {@code null}，返回 {@code false}）。
     * @param cls 待检查的 cause 类（若为 {@code null} 或空，返回 {@code false}）。
     * @return 若某个 cause 异常是传入类的实例则返回 {@code true}，否则 {@code false}。
     */
    public static boolean hasCause(@Nullable Throwable t, @Nullable Class<?>... cls) {
        if (t == null || cls == null || cls.length == 0)
            return false;

        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());

        return searchForCause(t, dejaVu, cls) != null;
    }

    /**
     * 从 {@code 'cause'} 层级中取得第一个给定类的异常（若有）。
     *
     * @param t 待检查的 Throwable（若为 {@code null}，返回 {@code null}）。
     * @param cls 要获取的 cause 类（若为 {@code null}，返回 {@code null}）。
     * @param <T> 异常 cause 的类型。
     * @return 传入类的第一个 cause 异常，否则 {@code null}。
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
     * 遍历 {@link Throwable} 树，寻找第一个可被给定类型之一赋值的节点。
     * 通过跟踪已测试对象感知可能存在的循环引用。
     *
     * @param t Throwable。
     * @param dejaVu 已测试对象的跟踪集合。
     * @param types 候选类型。
     * @return 第一个满足条件的 Throwable，若无匹配则为 {@code null}。
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

    /**
     * 打印消息到标准输出（诊断信息用——vendor X.println）。
     *
     * @param msg 消息。
     */
    public static void println(String msg) {
        System.out.println(msg);
    }
}
