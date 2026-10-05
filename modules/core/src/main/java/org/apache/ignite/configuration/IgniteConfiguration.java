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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java
// Lesson 0.2 子集：注册表与配置定稿所需的七个字段
//（igniteInstanceName / nodeId / consistentId / igniteHome / workDirectory / gridLogger / userAttributes）
// + 拷贝构造器（initializeConfiguration 的"配置定稿副本"语义依赖它）。
// vendor 约 3200 行、200+ getter/setter（全部 SPI、cache、事务、持久化、部署……），
// 按课程纪律逐课生长；字段名与 vendor 原名一致（workDirectory 在 vendor 内部字段名为 igniteWorkDir）。

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * 启动 Ignite 实例的配置。
 * <p>
 * 本配置进入 {@code IgnitionEx.initializeConfiguration} 后会得到一个定稿副本：
 * nodeId 生成、工作目录解析、logger 代理包装、默认 SPI 注入等都发生在副本上，
 * 用户手里的原配置不被改动。
 */
public class IgniteConfiguration {
    /** 可用处理器数（池大小默认值的基数）。 */
    public static final int AVAILABLE_PROC_CNT = Runtime.getRuntime().availableProcessors();

    /** 默认公共池线程数（vendor：max(8, CPU 数)）。 */
    public static final int DFLT_PUBLIC_THREAD_CNT = Math.max(8, AVAILABLE_PROC_CNT);

    /** 默认线程空闲保活时间（毫秒）。 */
    public static final long DFLT_THREAD_KEEP_ALIVE_TIME = 60_000L;

    /** 实例名（{@code null} 表示默认无名实例）。 */
    private String igniteInstanceName;

    /** Ignite 安装主目录。 */
    private String igniteHome;

    /** 节点 ID（未设置时启动期生成随机 UUID）。 */
    private UUID nodeId;

    /** 拓扑一致 ID（持久化场景的稳定节点标识）。 */
    private Serializable consistentId;

    /** 工作目录（日志/PDS/metastorage 等落盘的根）。 */
    private String igniteWorkDir;

    /** Grid logger。 */
    private IgniteLogger log;

    /** 用户属性（随节点属性广播）。 */
    private Map<String, ?> userAttrs;

    /** 公共线程池大小。 */
    private int pubPoolSize = DFLT_PUBLIC_THREAD_CNT;

    /**
     * 创建默认配置。
     */
    public IgniteConfiguration() {
        // 空实现。
    }

    /**
     * 拷贝构造器：按字段复制既有配置（vendor 按字母序全量复制；此处复制当前子集）。
     *
     * @param cfg 既有配置。
     */
    public IgniteConfiguration(IgniteConfiguration cfg) {
        assert cfg != null;

        consistentId = cfg.getConsistentId();
        igniteHome = cfg.getIgniteHome();
        igniteInstanceName = cfg.getIgniteInstanceName();
        igniteWorkDir = cfg.getWorkDirectory();
        log = cfg.getGridLogger();
        nodeId = cfg.getNodeId();
        pubPoolSize = cfg.getPublicThreadPoolSize();
        userAttrs = cfg.getUserAttributes();
    }

    /**
     * 取实例名。
     *
     * @return 实例名；默认实例为 {@code null}。
     */
    public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * 设置实例名。
     *
     * @param igniteInstanceName 实例名。
     * @return {@code this}（链式调用）。
     */
    public IgniteConfiguration setIgniteInstanceName(String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;

        return this;
    }

    /**
     * 取 Ignite 安装主目录。
     *
     * @return 主目录；未设置时为 {@code null}（启动期解析）。
     */
    public String getIgniteHome() {
        return igniteHome;
    }

    /**
     * 设置 Ignite 安装主目录。
     *
     * @param igniteHome 主目录。
     * @return {@code this}。
     */
    public IgniteConfiguration setIgniteHome(String igniteHome) {
        this.igniteHome = igniteHome;

        return this;
    }

    /**
     * 取节点 ID。
     *
     * @return 节点 ID；未显式设置时为 {@code null}（启动期生成）。
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * 设置节点 ID（不设置则每次启动生成随机 UUID——同一配置反复启动会得到不同节点）。
     *
     * @param nodeId 节点 ID。
     * @return {@code this}。
     */
    public IgniteConfiguration setNodeId(UUID nodeId) {
        this.nodeId = nodeId;

        return this;
    }

    /**
     * 取拓扑一致 ID。
     *
     * @return consistentId；未设置时为 {@code null}。
     */
    @Nullable public Serializable getConsistentId() {
        return consistentId;
    }

    /**
     * 设置拓扑一致 ID（持久化集群的目录归属键）。
     *
     * @param consistentId consistentId。
     * @return {@code this}。
     */
    public IgniteConfiguration setConsistentId(@Nullable Serializable consistentId) {
        this.consistentId = consistentId;

        return this;
    }

    /**
     * 取工作目录。
     *
     * @return 工作目录；未设置时为 {@code null}（启动期解析）。
     */
    public String getWorkDirectory() {
        return igniteWorkDir;
    }

    /**
     * 设置工作目录。
     *
     * @param workDir 工作目录。
     * @return {@code this}。
     */
    public IgniteConfiguration setWorkDirectory(String workDir) {
        this.igniteWorkDir = workDir;

        return this;
    }

    /**
     * 取 grid logger。
     *
     * @return logger；未设置时为 {@code null}（启动期落 JavaLogger）。
     */
    public IgniteLogger getGridLogger() {
        return log;
    }

    /**
     * 设置 grid logger。
     *
     * @param log logger。
     * @return {@code this}。
     */
    public IgniteConfiguration setGridLogger(IgniteLogger log) {
        this.log = log;

        return this;
    }

    /**
     * 取用户属性。
     *
     * @return 用户属性；未设置为 {@code null}（启动期补空 Map）。
     */
    public Map<String, ?> getUserAttributes() {
        return userAttrs;
    }

    /**
     * 设置用户属性。
     *
     * @param userAttrs 用户属性。
     * @return {@code this}。
     */
    public IgniteConfiguration setUserAttributes(Map<String, ?> userAttrs) {
        this.userAttrs = userAttrs;

        return this;
    }

    /**
     * 取公共线程池大小。
     *
     * @return 公共池大小。
     */
    public int getPublicThreadPoolSize() {
        return pubPoolSize;
    }

    /**
     * 设置公共线程池大小。
     *
     * @param pubPoolSize 公共池大小（须大于 0）。
     * @return {@code this}（链式调用）。
     */
    public IgniteConfiguration setPublicThreadPoolSize(int pubPoolSize) {
        this.pubPoolSize = pubPoolSize;

        return this;
    }

    @Override public String toString() {
        return "IgniteConfiguration [igniteInstanceName=" + igniteInstanceName
            + ", nodeId=" + nodeId
            + ", consistentId=" + consistentId
            + ", igniteHome=" + igniteHome
            + ", workDirectory=" + igniteWorkDir + ']';
    }
}
