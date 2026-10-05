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

// 对应 vendor: vendors/ignite/modules/binary/impl/src/main/java/org/apache/ignite/marshaller/jdk/JdkMarshallerObjectInputStream.java
// Lesson 0.3 子集：完整类（forName 解析 + Dummy 占位还原）。

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshallers;
import org.jetbrains.annotations.Nullable;

/**
 * 自定义 JDK 对象输入流。
 */
class JdkMarshallerObjectInputStream extends ObjectInputStream {
    /** 类加载器。 */
    private final ClassLoader clsLdr;

    /** 类名过滤器。 */
    private final IgnitePredicate<String> clsFilter;

    /**
     * @param in 父输入流。
     * @param clsLdr 自定义类加载器。
     * @param clsFilter 类过滤器。
     * @throws IOException 初始化失败时抛出。
     */
    JdkMarshallerObjectInputStream(InputStream in, ClassLoader clsLdr,
        @Nullable IgnitePredicate<String> clsFilter) throws IOException {
        super(in);

        assert clsLdr != null;

        this.clsLdr = clsLdr;
        this.clsFilter = clsFilter;

        enableResolveObject(true);
    }

    /** {@inheritDoc} */
    @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
        // 注意：不得改为 'clsLoader.loadClass()'！
        // 必须用 'Class.forName()'——数组类在某些场景下 loadClass 会抛莫名的
        // ClassNotFoundException（vendor 原注释）。
        return CommonUtils.forName(desc.getName(), clsLdr, clsFilter, Marshallers.USE_CACHE.get());
    }

    /** {@inheritDoc} */
    @Override protected Object resolveObject(Object o) throws IOException {
        if (o != null && o.getClass().equals(JdkMarshallerDummySerializable.class))
            return new Object();

        return super.resolveObject(o);
    }
}
