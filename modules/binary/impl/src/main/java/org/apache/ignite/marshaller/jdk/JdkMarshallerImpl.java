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

// 对应 vendor: vendors/ignite/modules/binary/impl/src/main/java/org/apache/ignite/marshaller/jdk/JdkMarshallerImpl.java
// Lesson 0.3：完整实现（JDK 序列化 + 类过滤 + 排除替换 + 节点名感知）。

package org.apache.ignite.marshaller.jdk;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.AbstractNodeNameAwareMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * 基于 JDK 序列化机制的 {@link org.apache.ignite.marshaller.Marshaller} 实现。
 * <p>
 * 无必填配置参数。需显式配置以覆盖默认 binary marshaller。
 */
public class JdkMarshallerImpl extends AbstractNodeNameAwareMarshaller implements JdkMarshaller {
    /** 类名过滤器。 */
    private final IgnitePredicate<String> clsFilter;

    /**
     * 默认构造。
     * <p>
     * 谨慎使用：本构造创建的实例<b>未启用类过滤</b>。若在服务端用它反序列化
     * 来自网络的用户数据，可能引入安全风险。
     */
    public JdkMarshallerImpl() {
        this(null);
    }

    /**
     * @param clsFilter 类名过滤器。
     */
    public JdkMarshallerImpl(@Nullable IgnitePredicate<String> clsFilter) {
        this.clsFilter = clsFilter;
    }

    /** {@inheritDoc} */
    @Override protected void marshal0(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        assert out != null;

        try (ObjectOutputStream objOut = new JdkMarshallerObjectOutputStream(
            new JdkMarshallerOutputStreamWrapper(out))) {
            // 只序列化对象本身，不带类加载器（vendor 原注释语义）。
            objOut.writeObject(obj);

            objOut.flush();
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to serialize object: " + obj, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected byte[] marshal0(@Nullable Object obj) throws IgniteCheckedException {
        try (GridByteArrayOutputStream out = new GridByteArrayOutputStream(DFLT_BUFFER_SIZE)) {
            marshal0(obj, out);

            return out.toByteArray();
        }
    }

    /** {@inheritDoc} */
    @Override protected <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert in != null;

        if (clsLdr == null)
            clsLdr = getClass().getClassLoader();

        try (ObjectInputStream objIn = new JdkMarshallerObjectInputStream(
            new JdkMarshallerInputStreamWrapper(in), clsLdr, clsFilter)) {
            return (T)objIn.readObject();
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to find class with given class loader for unmarshalling " +
                "(make sure same versions of all classes are available on all nodes or enable peer-class-loading) " +
                "[clsLdr=" + clsLdr + ", cls=" + e.getMessage() + ']', e);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to deserialize object with given class loader: " + clsLdr, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected <T> T unmarshal0(byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        try (GridByteArrayInputStream in = new GridByteArrayInputStream(arr, 0, arr.length)) {
            return unmarshal0(in, clsLdr);
        }
    }

    /** {@inheritDoc} */
    @Override public void onUndeploy(ClassLoader ldr) {
        // 无操作。
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "JdkMarshallerImpl [clsFilter=" + (clsFilter != null ? "set" : "null") + ']';
    }
}
