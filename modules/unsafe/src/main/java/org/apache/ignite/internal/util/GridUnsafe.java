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
 * <p>Wrapper for {@link sun.misc.Unsafe} class.</p>
 *
 * <p>
 * The following statements for memory access operations  are true:
 * <ul>
 * <li>All {@code putXxx(long addr, xxx val)}, {@code getXxx(long addr)}, {@code putXxx(byte[] arr, long off, xxx val)},
 * {@code getXxx(byte[] arr, long off)} and corresponding methods with {@code LE} suffix are alignment aware
 * and can be safely used with unaligned pointers.</li>
 * <li>All {@code putXxxField(Object obj, long fieldOff, xxx val)} and {@code getXxxField(Object obj, long fieldOff)}
 * methods are not alignment aware and can't be safely used with unaligned pointers. This methods can be safely used
 * for object field values access because all object fields addresses are aligned.</li>
 * <li>All {@code putXxxLE(...)} and {@code getXxxLE(...)} methods assumes that byte order is fixed as little-endian
 * while native byte order is big-endian. So it is client code responsibility to check native byte order before
 * invoking of this methods.</li>
 * </ul>
 * </p>
 */
public abstract class GridUnsafe {
    /** */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /** Unsafe. */
    private static final Unsafe UNSAFE = unsafe();

    /** Big endian. */
    public static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /**
     * @return Instance of Unsafe class.
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
