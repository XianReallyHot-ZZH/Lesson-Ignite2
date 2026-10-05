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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridKernalContext.java
// Lesson 0.3 子集：组件迭代 + 标识/日志/配置 + gateway + marshaller + pool/timeout 访问器。
// vendor 接口的 40+ 组件访问器（discovery()/io()/cache()/……）随各自课程逐个生长；
// state()/dynamicLogger()/exceptionRegistry() 等基础设施访问器同此。

package org.apache.ignite.internal;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.marshaller.MarshallerContext;
import org.jetbrains.annotations.Nullable;

/**
 * kernal 上下文：全部组件的注册表与公共访问入口。
 * <p>
 * 实现 {@code Iterable<GridComponent>}——迭代顺序即组件注册顺序
 * （亦即 vendor 启动时间线的子序列，见 course-map 生长不变量）。
 */
public interface GridKernalContext extends Iterable<GridComponent> {
    /**
     * @return 按加入顺序排列的全部组件。
     */
    public List<GridComponent> components();

    /**
     * @return 本地节点 ID。
     */
    public UUID localNodeId();

    /**
     * @return Ignite 实例名。
     */
    public String igniteInstanceName();

    /**
     * 按类别取 logger。
     *
     * @param ctgr 类别。
     * @return logger。
     */
    public IgniteLogger log(String ctgr);

    /**
     * 按类取 logger。
     *
     * @param cls 类。
     * @return logger。
     */
    public IgniteLogger log(Class<?> cls);

    /**
     * @return grid 配置。
     */
    public IgniteConfiguration config();

    /**
     * @return 本上下文所属的 grid 实例。
     */
    public IgniteEx grid();

    /**
     * @return kernal gateway。
     */
    public GridKernalGateway gateway();

    /**
     * @return marshaller（2.18 固定为 {@link BinaryMarshaller}）。
     */
    public BinaryMarshaller marshaller();

    /**
     * @return marshaller 上下文。
     */
    public MarshallerContext marshallerContext();

    /**
     * @return 线程池 processor。
     */
    public PoolProcessor pool();

    /**
     * @return 超时 processor。
     */
    public GridTimeoutProcessor timeout();

    /**
     * @return worker 注册表（可为 {@code null}——最小闭包未提供）。
     */
    @Nullable public WorkersRegistry workersRegistry();

    /**
     * @return 线程池缺省未捕获异常处理器（可为 {@code null}）。
     */
    @Nullable public Thread.UncaughtExceptionHandler uncaughtExceptionHandler();
}
