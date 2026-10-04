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

// 对应 vendor: vendors/ignite/modules/unsafe/src/main/java/org/apache/ignite/internal/util/GridUnsafe.java
// Lesson 0.1 子集：仅静态初始化（反射获取 theUnsafe）+ 两个公开字节序常量。
// Unsafe.getUnsafe() 在用户代码里抛 SecurityException，落回 theUnsafe 反射——vendor 同路径。
// 页大小/offheap/数组偏移/DirectBuffer 清理等常量与方法随持久化课弧（章 7）生长。

package org.apache.ignite.internal.util;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import sun.misc.Unsafe;

/**
 * <p>{@link sun.misc.Unsafe} 类的封装。</p>
 *
 * <p>
 * 对内存访问操作，以下陈述成立：
 * <ul>
 * <li>所有 {@code putXxx(long addr, xxx val)}、{@code getXxx(long addr)}、
 * {@code putXxx(byte[] arr, long off, xxx val)}、{@code getXxx(byte[] arr, long off)}
 * 及带 {@code LE} 后缀的对应方法都是对齐感知的，可安全用于未对齐指针。</li>
 * <li>所有 {@code putXxxField(Object obj, long fieldOff, xxx val)} 与
 * {@code getXxxField(Object obj, long fieldOff)} 方法非对齐感知，不能安全用于未对齐指针；
 * 但用于对象字段值的访问是安全的，因为对象字段地址总是对齐的。</li>
 * <li>所有 {@code putXxxLE(...)} 与 {@code getXxxLE(...)} 方法假定字节序固定为小端，
 * 而原生字节序为大端；调用方有责任在调用这些方法前检查原生字节序。</li>
 * </ul>
 * </p>
 */
public abstract class GridUnsafe {
    /** */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /** Unsafe 实例。 */
    private static final Unsafe UNSAFE = unsafe();

    /** 是否大端。 */
    public static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /**
     * @return Unsafe 类实例。
     */
    private static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Unsafe>() {
                        @Override public Unsafe run() throws Exception {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");

                            f.setAccessible(true);

                            return (Unsafe)f.get(null);
                        }
                    });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }
}
