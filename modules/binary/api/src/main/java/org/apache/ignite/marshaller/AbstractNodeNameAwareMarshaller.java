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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/AbstractNodeNameAwareMarshaller.java
// Lesson 0.3 子集：完整类。模板方法在每次 marshal/unmarshal 前后切换/恢复
// 线程局部实例名，使序列化路径能感知当前实例（Ignition.localIgnite() 依赖）。

package org.apache.ignite.marshaller;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.CommonUtils;
import org.jetbrains.annotations.Nullable;

/**
 * 允许 {@code Ignition#localIgnite()} 调用的 marshaller 基类。
 */
public abstract class AbstractNodeNameAwareMarshaller extends AbstractMarshaller {
    /** 节点名是否已设置（只允许首次 nodeName 调用生效——多 kernal 共享 marshaller 的仲裁）。 */
    private volatile boolean nodeNameSet;

    /** 节点名（未设置时为空串哨兵）。 */
    private volatile String nodeName = CommonUtils.LOC_IGNITE_NAME_EMPTY;

    /** {@inheritDoc} */
    @Override public void nodeName(@Nullable String nodeName) {
        if (!nodeNameSet) {
            this.nodeName = nodeName;

            nodeNameSet = true;
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
        String oldNodeName = CommonUtils.setCurrentIgniteName(nodeName);

        try {
            return marshal0(obj);
        }
        finally {
            CommonUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        String oldNodeName = CommonUtils.setCurrentIgniteName(nodeName);

        try {
            marshal0(obj, out);
        }
        finally {
            CommonUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        String oldNodeName = CommonUtils.setCurrentIgniteName(nodeName);

        try {
            return unmarshal0(arr, clsLdr);
        }
        finally {
            CommonUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        String oldNodeName = CommonUtils.setCurrentIgniteName(nodeName);

        try {
            return unmarshal0(in, clsLdr);
        }
        finally {
            CommonUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /**
     * 把对象 marshal 到输出流。本方法不应关闭给定输出流。
     *
     * @param obj 待 marshal 对象。
     * @param out 目标输出流。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    protected abstract void marshal0(@Nullable Object obj, OutputStream out) throws IgniteCheckedException;

    /**
     * 把对象 marshal 为 byte 数组。
     *
     * @param obj 待 marshal 对象。
     * @return byte 数组。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    protected abstract byte[] marshal0(@Nullable Object obj) throws IgniteCheckedException;

    /**
     * 用给定类加载器从输入流 unmarshal 对象。
     *
     * @param <T> 反序列化对象类型。
     * @param in 输入流。
     * @param clsLdr 类加载器。
     * @return 反序列化对象。
     * @throws IgniteCheckedException unmarshal 失败时抛出。
     */
    protected abstract <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * 用给定类加载器从 byte 数组 unmarshal 对象。
     *
     * @param <T> 反序列化对象类型。
     * @param arr byte 数组。
     * @param clsLdr 类加载器。
     * @return 反序列化对象。
     * @throws IgniteCheckedException unmarshal 失败时抛出。
     */
    protected abstract <T> T unmarshal0(byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException;
}
