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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/GridArgumentCheck.java
// Lesson 0.2 子集：notNull（1~4 参）+ ensure；notEmpty 家族（Collection/Map/数组）随消费者出现再补。

package org.apache.ignite.internal.util;

import org.jetbrains.annotations.Nullable;

/**
 * 参数校验助手。消息前缀与 vendor 保持一致（"Ouch!" 风格），
 * 便于与官方测试/日志中的断言文本对照。
 */
public class GridArgumentCheck {
    /** 空指针错误消息前缀。 */
    public static final String NULL_MSG_PREFIX = "Ouch! Argument cannot be null: ";

    /** 非法参数错误消息前缀。 */
    private static final String INVALID_ARG_MSG_PREFIX = "Ouch! Argument is invalid: ";

    /**
     * 确保参数非空。
     *
     * @param val 待检值。
     * @param name 参数名。
     * @throws NullPointerException 若参数为 {@code null}。
     */
    public static void notNull(@Nullable Object val, String name) {
        if (val == null)
            throw new NullPointerException(NULL_MSG_PREFIX + name);
    }

    /**
     * 确保两个参数均非空。
     *
     * @param val1 第一个待检值。
     * @param name1 第一个参数名。
     * @param val2 第二个待检值。
     * @param name2 第二个参数名。
     */
    public static void notNull(Object val1, String name1, Object val2, String name2) {
        notNull(val1, name1);
        notNull(val2, name2);
    }

    /**
     * 确保三个参数均非空。
     *
     * @param val1 第一个待检值。
     * @param name1 第一个参数名。
     * @param val2 第二个待检值。
     * @param name2 第二个参数名。
     * @param val3 第三个待检值。
     * @param name3 第三个参数名。
     */
    public static void notNull(Object val1, String name1, Object val2, String name2,
        Object val3, String name3) {
        notNull(val1, name1);
        notNull(val2, name2);
        notNull(val3, name3);
    }

    /**
     * 确保四个参数均非空。
     *
     * @param val1 第一个待检值。
     * @param name1 第一个参数名。
     * @param val2 第二个待检值。
     * @param name2 第二个参数名。
     * @param val3 第三个待检值。
     * @param name3 第三个参数名。
     * @param val4 第四个待检值。
     * @param name4 第四个参数名。
     */
    public static void notNull(Object val1, String name1, Object val2, String name2,
        Object val3, String name3, Object val4, String name4) {
        notNull(val1, name1);
        notNull(val2, name2);
        notNull(val3, name3);
        notNull(val4, name4);
    }

    /**
     * 确保条件成立。
     *
     * @param cond 条件。
     * @param desc 条件描述（用于错误消息）。
     * @throws IllegalArgumentException 若条件不成立。
     */
    public static void ensure(boolean cond, String desc) {
        if (!cond)
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + desc);
    }
}
