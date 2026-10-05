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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/Marshallers.java
// Lesson 0.3 子集：jdk() 工厂 + USE_CACHE ThreadLocal + marshal 静态包装。
// vendor 另有 optimized()/optimized(boolean)（OptimizedMarshaller 属部署课 12.x，届时补）。

package org.apache.ignite.marshaller;

import java.io.OutputStream;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * 创建 {@link Marshaller} 实现的静态门面（经 ServiceLoader 找 impl 模块的工厂）。
 */
public final class Marshallers {
    /** 当前线程的反序列化是否使用类缓存。 */
    public static final ThreadLocal<Boolean> USE_CACHE = ThreadLocal.withInitial(() -> Boolean.TRUE);

    /** 工厂实现（impl 模块经 META-INF/services 注册）。 */
    private static final MarshallersFactory factory;

    static {
        Iterator<MarshallersFactory> factories = CommonUtils.loadService(MarshallersFactory.class).iterator();

        A.ensure(
            factories.hasNext(),
            "Implementation for MarshallersFactory service not found. Please add ignite-binary-impl to classpath"
        );

        factory = factories.next();
    }

    /**
     * 单例约束。
     */
    private Marshallers() {
        // 空实现。
    }

    /**
     * @return 默认 {@link JdkMarshaller} 实例。
     */
    public static JdkMarshaller jdk() {
        return factory.jdk();
    }

    /**
     * @param clsFilter 类过滤器。
     * @return 带类过滤器的 {@link JdkMarshaller} 实例。
     */
    public static JdkMarshaller jdk(@Nullable IgnitePredicate<String> clsFilter) {
        return factory.jdk(clsFilter);
    }

    /**
     * 把对象 marshal 为 byte 数组（包装调用，保证失败时必抛 {@link IgniteCheckedException}）。
     *
     * @param marsh marshaller。
     * @param obj 待 marshal 对象。
     * @return byte 数组。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    public static byte[] marshal(Marshaller marsh, @Nullable Object obj) throws IgniteCheckedException {
        assert marsh != null;

        try {
            return marsh.marshal(obj);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * 把对象 marshal 到输出流（包装调用，保证失败时必抛 {@link IgniteCheckedException}）。
     *
     * @param marsh marshaller。
     * @param obj 待 marshal 对象。
     * @param out 目标输出流。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    public static void marshal(Marshaller marsh, @Nullable Object obj, OutputStream out)
        throws IgniteCheckedException {
        assert marsh != null;

        try {
            marsh.marshal(obj, out);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }
}
