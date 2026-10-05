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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryMarshaller.java
// Lesson 0.3：delegate 占位形态（marshaller 演进主线票 #2 的约束）——
// 2.18 的 BinaryMarshaller 持 GridBinaryMarshaller impl（binary 线协议），
// 真格式随 Lesson 3.10（任意对象过线）/13.1（binary 完整化）接入，
// 此期间 marshal/unmarshal 全部委托 JdkMarshaller，保证 ctx.marshaller() 通道可用。
// vendor 的 setBinaryContext(BinaryContext)/binaryMarshaller() 访问器在真 impl 接入时补。
// available()（JVM 私有 API 探测）与 vendor 一致保留。

package org.apache.ignite.internal.binary;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.AbstractNodeNameAwareMarshaller;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

/**
 * 以 binary 格式序列化/反序列化所有对象的 {@link org.apache.ignite.marshaller.Marshaller}
 * 实现（0.3 占位：委托 JDK 序列化）。
 */
public class BinaryMarshaller extends AbstractNodeNameAwareMarshaller {
    /** 委托实现（占位期的真实通道）。 */
    private final JdkMarshaller dfltMarsh = Marshallers.jdk();

    /**
     * 检测 {@code BinaryMarshaller} 能否在当前 JVM 工作。
     * <p>
     * 由于 {@code BinaryMarshaller} 使用 JVM 私有 API（不保证所有 JVM 可用），
     * 构造前应调用本方法确认。
     *
     * @return 可用返回 {@code true}。
     */
    @SuppressWarnings({"TypeParameterExtendsFinalClass", "ErrorNotRethrown"})
    public static boolean available() {
        try {
            Class<? extends Unsafe> unsafeCls = Unsafe.class;

            unsafeCls.getMethod("allocateInstance", Class.class);
            unsafeCls.getMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
        catch (NoClassDefFoundError ignored) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override protected byte[] marshal0(@Nullable Object obj) throws IgniteCheckedException {
        return dfltMarsh.marshal(obj);
    }

    /** {@inheritDoc} */
    @Override protected void marshal0(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        dfltMarsh.marshal(obj, out);
    }

    /** {@inheritDoc} */
    @Override protected <T> T unmarshal0(byte[] bytes, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        return dfltMarsh.unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override protected <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        return dfltMarsh.unmarshal(in, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public void onUndeploy(ClassLoader ldr) {
        // JDK 委托路径无需清理。
    }
}
