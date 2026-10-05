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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/MarshallerContext.java
// Lesson 0.3 子集：接口完整（registerClassName 的 4 参默认重载 + deprecated 3 参形态都保留）。

package org.apache.ignite.marshaller;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * marshaller 上下文：typeId→类名映射的注册与查询中心。
 */
public interface MarshallerContext {
    /**
     * 在 marshaller 上下文中<b>集群级</b>注册 typeId→类名映射。
     * <p>
     * 本方法<b>保证</b>映射已送达集群全部节点，并阻塞调用线程直至完成。
     * （集群级投递由 GridMarshallerMappingProcessor + discovery/communication 总线承载，
     * 属 Lesson 12.3；0.3 的实现是本地空注册表。）
     *
     * @param platformId 平台 ID（java/.NET……）。
     * @param typeId 类型 ID。
     * @param clsName 类名。
     * @param failIfUnregistered 为 {@code true} 时以 UnregisteredBinaryTypeException 快速失败
     *      而非同步等待注册 future 完成。
     * @return 注册成功返回 {@code true}。
     * @throws IgniteCheckedException 出错时抛出。
     */
    public default boolean registerClassName(
        byte platformId,
        int typeId,
        String clsName,
        boolean failIfUnregistered
    ) throws IgniteCheckedException {
        return registerClassName(platformId, typeId, clsName);
    }

    /**
     * {@link #registerClassName(byte, int, java.lang.String, boolean)} 的短参形态。
     *
     * @param platformId 平台 ID。
     * @param typeId 类型 ID。
     * @param clsName 类名。
     * @return 注册成功返回 {@code true}。
     * @throws IgniteCheckedException 出错时抛出。
     * @deprecated 改用 {@link #registerClassName(byte, int, java.lang.String, boolean)}。
     */
    @Deprecated
    public boolean registerClassName(
        byte platformId,
        int typeId,
        String clsName
    ) throws IgniteCheckedException;

    /**
     * 在 marshaller 上下文中<b>仅本地节点</b>注册 typeId→类名映射。
     * <p>
     * <b>不保证</b>映射出现在其他节点。当存在另一可靠映射来源（磁盘持久化 metadata）时可安全使用。
     *
     * @param platformId 平台 ID。
     * @param typeId 类型 id。
     * @param clsName 类名。
     * @return 注册成功返回 {@code true}。
     * @throws IgniteCheckedException 出错时抛出。
     */
    public boolean registerClassNameLocally(byte platformId, int typeId, String clsName)
        throws IgniteCheckedException;

    /**
     * 按 typeId 取类。
     *
     * @param typeId 类型 ID。
     * @param ldr 类加载器。
     * @return 类。
     * @throws ClassNotFoundException 类不存在时抛出。
     * @throws IgniteCheckedException 其他错误时抛出。
     */
    public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException;

    /**
     * 按 (platformId, typeId) 取类名。
     *
     * @param platformId 平台 ID。
     * @param typeId 类型 ID。
     * @return 类名。
     * @throws ClassNotFoundException 类不存在时抛出。
     * @throws IgniteCheckedException 其他错误时抛出。
     */
    public String getClassName(byte platformId, int typeId) throws ClassNotFoundException, IgniteCheckedException;

    /**
     * 判断给定类型是否为系统类型（JDK 类或 Ignite 类）。
     *
     * @param typeName 类型名。
     * @return 是系统类型返回 {@code true}。
     */
    public boolean isSystemType(String typeName);

    /**
     * @return 类名过滤器。
     */
    public IgnitePredicate<String> classNameFilter();

    /**
     * @return JDK marshaller 实例。
     */
    public JdkMarshaller jdkMarshaller();
}
