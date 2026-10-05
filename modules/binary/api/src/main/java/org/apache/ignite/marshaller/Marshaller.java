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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/Marshaller.java
// Lesson 0.3 子集：接口完整（Spring 配置示例段落按 ADR 0001 排除）。

package org.apache.ignite.marshaller;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@code Marshaller} 允许在网格内 marshal/unmarshal 对象。它为所有跨网络发送
 * 或以其他方式序列化的实例提供序列化/反序列化机制。
 * <p>
 * Ignite 提供以下 {@code Marshaller} 实现：
 * <ul>
 * <li>默认 binary marshaller（未显式配置时使用，见 {@code IgniteBinary}）；</li>
 * <li>{@link org.apache.ignite.marshaller.jdk.JdkMarshaller}。</li>
 * </ul>
 */
public interface Marshaller {
    /**
     * 设置 marshaller 上下文。
     *
     * @param ctx Marshaller 上下文。
     */
    public void setContext(MarshallerContext ctx);

    /**
     * 设置节点名。
     *
     * @param nodeName 节点名。
     */
    public void nodeName(@Nullable String nodeName);

    /**
     * 把对象 marshal 到输出流。本方法不应关闭给定输出流。
     *
     * @param obj 待 marshal 对象（{@code null} marshal 为二进制 {@code null} 表示）。
     * @param out 目标输出流。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException;

    /**
     * 把对象 marshal 为 byte 数组。
     *
     * @param obj 待 marshal 对象（{@code null} marshal 为二进制 {@code null} 表示）。
     * @return byte 数组。
     * @throws IgniteCheckedException marshal 失败时抛出。
     */
    public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException;

    /**
     * 用给定类加载器从输入流 unmarshal 对象。本方法不应关闭给定输入流。
     *
     * @param <T> 反序列化对象类型。
     * @param in 输入流。
     * @param clsLdr 非 {@code null} 时用该类加载器反序列化。
     * @return 反序列化对象。
     * @throws IgniteCheckedException unmarshal 失败时抛出。
     */
    public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * 用给定类加载器从 byte 数组 unmarshal 对象。
     *
     * @param <T> 反序列化对象类型。
     * @param arr byte 数组。
     * @param clsLdr 非 {@code null} 时用该类加载器反序列化。
     * @return 反序列化对象。
     * @throws IgniteCheckedException unmarshal 失败时抛出。
     */
    public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * 类加载器被 undeploy 时的回调。
     * <p>
     * 某些 marshaller 可能需要清理内部状态中引用该类加载器的部分。
     *
     * @param ldr 被 undeploy 的类加载器。
     */
    public void onUndeploy(ClassLoader ldr);
}
