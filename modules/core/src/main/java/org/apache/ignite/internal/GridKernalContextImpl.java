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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridKernalContextImpl.java
// Lesson 0.3 子集：组件列表 + add 的 instanceof 派发 + 标识/日志/gateway/marshaller 持有。
// 与 vendor 的差异（按归属课程排列，接入时按 vendor 形态补）：
// - 构造器裁剪了 plugins/clsFilter/pauseDetector 参数（插件系统/marshaller 类过滤器/长停顿
//   检测分别属插件课/12.3/运维面）；workersRegistry/hnd 保留（0.3 传入最小实现）；
// - add() 的派发分支当前只认 PoolProcessor/GridTimeoutProcessor，新组件接入时在对应
//   "managers/processors" 段落按 vendor 顺序插入分支；
// - 节点属性 attrs、segFlag、performance 建议等字段随章 2+ 接入；
// - marsh 固定 new BinaryMarshaller()（与 vendor 一致）。

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
 * kernal 上下文默认实现。
 */
public class GridKernalContextImpl implements GridKernalContext {
    /** 组件列表（注册顺序）。 */
    private final List<GridComponent> comps = new LinkedList<>();

    /** 节点属性（章 2 fillNodeAttributes 接入）。 */
    private final Map<String, Object> attrs = new HashMap<>();

    /** worker 注册表。 */
    private final WorkersRegistry workersRegistry;

    /** 线程池缺省未捕获异常处理器。 */
    private final Thread.UncaughtExceptionHandler hnd;

    /** 所属 grid。 */
    private final IgniteEx grid;

    /** 配置。 */
    private final IgniteConfiguration cfg;

    /** gateway。 */
    private final GridKernalGateway gw;

    /** marshaller 上下文（0.3 空注册表形态）。 */
    private final MarshallerContextImpl marshCtx;

    /** marshaller（vendor 同款：context 固定持有 BinaryMarshaller 实例）。 */
    private final BinaryMarshaller marsh = new BinaryMarshaller();

    /** pool processor。 */
    private PoolProcessor poolProc;

    /** timeout processor。 */
    private GridTimeoutProcessor timeProc;

    /**
     * 创建 kernal 上下文。
     *
     * @param grid 被本 kernal 管理的 grid 实例。
     * @param cfg grid 配置。
     * @param gw kernal gateway。
     * @param workerRegistry worker 注册表（{@code null} 允许——最小闭包可不注册 worker）。
     * @param hnd 线程池缺省未捕获异常处理器（{@code null} 允许）。
     */
    protected GridKernalContextImpl(
        IgniteEx grid,
        IgniteConfiguration cfg,
        GridKernalGateway gw,
        @Nullable WorkersRegistry workerRegistry,
        @Nullable Thread.UncaughtExceptionHandler hnd
    ) {
        assert grid != null;
        assert cfg != null;
        assert gw != null;

        this.grid = grid;
        this.cfg = cfg;
        this.gw = gw;
        this.workersRegistry = workerRegistry;
        this.hnd = hnd;

        marshCtx = new MarshallerContextImpl();
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridComponent> iterator() {
        return comps.iterator();
    }

    /** {@inheritDoc} */
    @Override public List<GridComponent> components() {
        return Collections.unmodifiableList(comps);
    }

    /**
     * 注册组件（加入组件列表）。
     *
     * @param comp 待注册组件。
     */
    public void add(GridComponent comp) {
        add(comp, true);
    }

    /**
     * 注册组件。按 instanceof 把组件派发到对应访问器字段
     * （vendor 同构：先派发、后决定是否入列表）。
     *
     * @param comp 待注册组件。
     * @param addToList 为 {@code true} 时加入组件列表（discovery manager 这类
     *      "先注册后启动"的特殊组件为 {@code false}——vendor 同款）。
     */
    @SuppressWarnings("ChainOfInstanceof")
    public void add(GridComponent comp, boolean addToList) {
        assert comp != null;

        /*
         * Managers（章 2 起接入，届时按 vendor 段落追加分支）。
         */

        /*
         * Processors（处理器）。
         */
        if (comp instanceof PoolProcessor)
            poolProc = (PoolProcessor)comp;
        else if (comp instanceof GridTimeoutProcessor)
            timeProc = (GridTimeoutProcessor)comp;
        else
            assert false : "Unknown manager class: " + comp.getClass();

        if (addToList)
            comps.add(comp);
    }

    /** {@inheritDoc} */
    @Override public UUID localNodeId() {
        return cfg.getNodeId();
    }

    /** {@inheritDoc} */
    @Override public String igniteInstanceName() {
        return cfg.getIgniteInstanceName();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(String ctgr) {
        return cfg.getGridLogger().getLogger(ctgr);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return cfg.getGridLogger().getLogger(cls);
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration config() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return grid;
    }

    /** {@inheritDoc} */
    @Override public GridKernalGateway gateway() {
        return gw;
    }

    /** {@inheritDoc} */
    @Override public BinaryMarshaller marshaller() {
        return marsh;
    }

    /** {@inheritDoc} */
    @Override public MarshallerContext marshallerContext() {
        return marshCtx;
    }

    /** {@inheritDoc} */
    @Override public PoolProcessor pool() {
        return poolProc;
    }

    /** {@inheritDoc} */
    @Override public GridTimeoutProcessor timeout() {
        return timeProc;
    }

    /**
     * @return worker 注册表（可为 {@code null}）。
     */
    @Nullable public WorkersRegistry workersRegistry() {
        return workersRegistry;
    }

    /**
     * @return 线程池缺省未捕获异常处理器（可为 {@code null}）。
     */
    @Nullable public Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
        return hnd;
    }

    /**
     * @return 节点属性表（章 2 fillNodeAttributes 写入）。
     */
    public Map<String, Object> nodeAttributes() {
        return attrs;
    }
}
