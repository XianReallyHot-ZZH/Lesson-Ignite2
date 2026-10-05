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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/MarshallerContextImpl.java
// Lesson 0.3：空注册表形态（决策票 #2/#3 约束）。本地 ConcurrentHashMap 注册/查询闭环；
// 未接入（按归属课程）：
// - 系统类型表（processSystemClasses 扫描 + sysTypesMap）——binary 元数据课（13.1）；
// - MarshallerMappingFileStore 磁盘持久化 + MarshallerMappingTransport 集群交换——12.3；
// - initializeMarshallerExclusions 的排除清单登记——GridLoggerProxy 等 kernel 类随各自课补；
// - platformId 维度（JAVA_ID=0 恒等）——多平台 ID 语义随 13.1。

package org.apache.ignite.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * marshaller 上下文实现（0.3 空注册表：typeId→类名的本地映射）。
 */
public class MarshallerContextImpl implements MarshallerContext {
    /** JAVA 平台 ID（vendor MarshallerPlatformIds.JAVA_ID）。 */
    private static final byte JAVA_ID = 0;

    /** typeId→类名注册表（0.3 不分 platform——只有 JAVA 平台在使用）。 */
    private final ConcurrentMap<Integer, String> mappings = new ConcurrentHashMap<>();

    /** JDK marshaller（jdkMarshaller() 访问器返回值）。 */
    private final JdkMarshaller jdkMarsh = Marshallers.jdk();

    /**
     * 初始化上下文（vendor 形态在此扫描系统类、建排除清单——见类头注释）。
     */
    public MarshallerContextImpl() {
        // 空注册表：无系统类型、无文件存储、无传输通道。
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(byte platformId, int typeId, String clsName)
        throws IgniteCheckedException {
        // 集群级投递（mappingTransport）属 12.3；当前等价本地注册。
        mappings.put(typeId, clsName);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassNameLocally(byte platformId, int typeId, String clsName)
        throws IgniteCheckedException {
        mappings.put(typeId, clsName);

        return true;
    }

    /** {@inheritDoc} */
    @Override public Class getClass(int typeId, @Nullable ClassLoader ldr)
        throws ClassNotFoundException, IgniteCheckedException {
        String clsName = mappings.get(typeId);

        if (clsName == null)
            throw new ClassNotFoundException("Class name is not registered for type id: " + typeId);

        return CommonUtils.forName(clsName, ldr, null, true);
    }

    /** {@inheritDoc} */
    @Override public String getClassName(byte platformId, int typeId)
        throws ClassNotFoundException, IgniteCheckedException {
        String clsName = mappings.get(typeId);

        if (clsName == null)
            throw new ClassNotFoundException("Class name is not registered for type id: " + typeId);

        return clsName;
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(String typeName) {
        // 系统类型表随 13.1（binary 元数据）接入，当前恒非系统类型。
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgnitePredicate<String> classNameFilter() {
        // 类名过滤器（IgniteMarshallerClassFilter）随 12.3 集群映射接入。
        return null;
    }

    /** {@inheritDoc} */
    @Override public JdkMarshaller jdkMarshaller() {
        return jdkMarsh;
    }
}
