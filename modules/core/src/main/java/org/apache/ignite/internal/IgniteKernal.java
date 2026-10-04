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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgniteKernal.java
// Lesson 0.2 子集：kernal 的"标识面"——只持定稿配置，实现 Ignite 的四个方法 + localNodeId。
// 0.3 的概念簇才是 kernal 容器本体：IgniteEx 接口、gateway 状态机、GridKernalContextImpl、
// GridComponent 先注册后启动契约、PoolProcessor/GridTimeoutProcessor/marshaller 三件套，
// 以及 vendor 全签名 start(cfg, errHnd, workerRegistry, oomeHnd, startTimer)。
// 构造器差异：vendor 有无参与带 GridSpringResourceContext 两个构造器——
// 复刻排除 Spring（ADR 0001），只保留无参形态。
// stop 目前是空链：组件逆序停止、生命周期 bean AFTER_NODE_STOP、gateway STOPPING→STOPPED
// 均是 0.4 的概念簇（本课 stop 只由注册表侧驱动）。

package org.apache.ignite.internal;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Ignite kernal——{@link Ignite} 接口的默认实现。
 * 0.2 阶段仅承载注册表语义所需的标识面；容器与组件编排自 0.3 起生长。
 */
public class IgniteKernal implements Ignite {
    /** 定稿配置（start 时赋值）。 */
    private volatile IgniteConfiguration cfg;

    /**
     * 创建 kernal（vendor 签名带 Spring 资源上下文参数，复刻排除 Spring，见类头注释）。
     */
    public IgniteKernal() {
        // 空实现。
    }

    /**
     * 启动 kernal。0.2 形态：只钉住定稿配置。
     * 0.3 将升级为 vendor 全签名并接入 gateway 状态机与组件容器。
     *
     * @param cfg 定稿配置（initializeConfiguration 的产物）。
     * @throws IgniteCheckedException 启动失败时抛出（0.3 起有实际抛出点）。
     */
    public void start(IgniteConfiguration cfg) throws IgniteCheckedException {
        assert cfg != null;

        this.cfg = cfg;
    }

    /**
     * 停止 kernal（空链——组件停链是 0.4 的概念簇）。
     *
     * @param cancel 是否取消运行中的作业（0.4 起生效）。
     */
    public void stop(boolean cancel) {
        assert cfg != null : "Kernal is not started.";
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cfg.getIgniteInstanceName();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /**
     * 取本地节点 ID（vendor 同为读 cfg；0.3 起改读 kernal context）。
     *
     * @return 本地节点 ID。
     */
    public UUID localNodeId() {
        assert cfg != null;

        return cfg.getNodeId();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        Ignition.stop(name(), true);
    }
}
