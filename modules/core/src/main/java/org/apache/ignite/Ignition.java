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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/Ignition.java
// Lesson 0.2 子集：工厂门面——start/getOrStart/state/stop/stopAll/restart/kill/ignite/allGrids/监听器。
// 显式排除（ADR 0001 首次应用——抛 UnsupportedOperationException 而非静默缺失）：
// - Spring 入口五件套：start(String/URL/InputStream) + loadSpringBean ×3（Spring 属外围 17 模块）；
// - localIgnite()：依赖 IgniteThread 基建（章 3 接入）；
// - startClient()：thin client 双端是 13.5 的收官里程碑。
// vendor 另有 sandbox 代理包装（wrapToProxyIfNeeded）——Ignite Sandbox 属 platforms 外围，不复刻。
// 静态方法壳全部保留 vendor 原形，实现委托 IgnitionEx。

package org.apache.ignite;

import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite 的工厂门面：控制 grid 实例生命周期、监听实例级状态事件。
 * <p>
 * 一个 JVM 可运行多个按名字区分的 Ignite 实例（{@code null} 名为默认实例），
 * 实例注册表语义（重名抛异常 / getOrStart 幂等 / 并发启动）由 {@link IgnitionEx} 承载。
 */
public class Ignition {
    /**
     * 供外部工具（如 Shell 脚本）使用的重启退出码——JVM 没有自重启的标准途径，
     * 由外部工具配合该码实现重启。标准 {@code ignite.{sh|bat}} 脚本支持此协议。
     */
    public static final int RESTART_EXIT_CODE = 250;

    /**
     * 供外部工具使用的停止退出码（不重启，直接杀掉 JVM 进程）。
     */
    public static final int KILL_EXIT_CODE = 130;

    /**
     * 单例约束。
     */
    protected Ignition() {
        // 空实现。
    }

    /**
     * 设置 client 模式线程局部标志。
     * <p>
     * {@link IgniteConfiguration} 的 client 模式未显式配置时，节点启动读取本标志。
     *
     * @param clientMode client 模式标志。
     */
    public static void setClientMode(boolean clientMode) {
        IgnitionEx.setClientMode(clientMode);
    }

    /**
     * 取 client 模式线程局部标志。
     *
     * @return client 模式标志。
     */
    public static boolean isClientMode() {
        return IgnitionEx.isClientMode();
    }

    /**
     * 取默认 grid 的状态。
     *
     * @return 默认 grid 状态。
     */
    public static IgniteState state() {
        return IgnitionEx.state();
    }

    /**
     * 取命名实例的状态；{@code null} 名取默认实例。
     *
     * @param name 实例名（可为 {@code null}）。
     * @return 实例状态。
     */
    public static IgniteState state(@Nullable String name) {
        return IgnitionEx.state(name);
    }

    /**
     * 停止默认 grid（等价 {@code stop(null, cancel)}）。
     *
     * @param cancel 是否取消运行中的作业。
     * @return 确实停止了返回 {@code true}；本就未启动返回 {@code false}。
     */
    public static boolean stop(boolean cancel) {
        return IgnitionEx.stop(cancel, null);
    }

    /**
     * 停止命名实例；{@code null} 名停止默认实例。
     *
     * @param name 实例名（可为 {@code null}）。
     * @param cancel 是否取消运行中的作业。
     * @return 找到并停止了返回 {@code true}；实例不存在返回 {@code false}。
     */
    public static boolean stop(String name, boolean cancel) {
        return IgnitionEx.stop(name, cancel, null, false);
    }

    /**
     * 停止当前 JVM 内 <b>全部</b> 已启动 grid。
     * <p>
     * 注意：逐个停止通常更安全——谁启动谁负责停止。
     *
     * @param cancel 是否取消运行中的作业。
     */
    public static void stopAll(boolean cancel) {
        IgnitionEx.stopAll(cancel, null);
    }

    /**
     * 重启 <b>全部</b> 已启动 grid（配合脚本重启协议，JVM 以
     * {@link #RESTART_EXIT_CODE} 退出后由脚本拉起新进程）。
     *
     * @param cancel 是否取消运行中的作业。
     * @see #RESTART_EXIT_CODE
     */
    public static void restart(boolean cancel) {
        IgnitionEx.restart(cancel);
    }

    /**
     * 停止 <b>全部</b> 已启动 grid 后以 {@link #KILL_EXIT_CODE} 强制退出 JVM。
     *
     * @param cancel 是否取消运行中的作业。
     * @see #KILL_EXIT_CODE
     */
    public static void kill(boolean cancel) {
        IgnitionEx.kill(cancel);
    }

    /**
     * 以默认配置启动 grid。
     *
     * @return 已启动的 grid。
     * @throws IgniteException 启动失败或默认实例已启动时抛出。
     */
    public static Ignite start() throws IgniteException {
        try {
            return IgnitionEx.start();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * 以给定配置启动 grid；配置中同名的实例已启动则抛异常。
     *
     * @param cfg grid 配置（不可为 {@code null}）。
     * @return 已启动的 grid。
     * @throws IgniteException 启动失败或同名实例已启动时抛出。
     */
    public static Ignite start(IgniteConfiguration cfg) throws IgniteException {
        try {
            return IgnitionEx.start(cfg);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * 以 Spring XML 配置文件路径启动 grid。
     * <p>
     * 复刻显式排除 Spring 入口（ADR 0001：Spring 属外围模块）。
     *
     * @param springCfgPath Spring XML 配置文件路径或 URL。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛——复刻范围不含 Spring。
     */
    public static Ignite start(String springCfgPath) throws IgniteException {
        throw new UnsupportedOperationException("Spring XML configuration entry points are out of the replica "
            + "scope (ADR-0001): use Ignition.start(IgniteConfiguration) instead.");
    }

    /**
     * 以 Spring XML 配置文件 URL 启动 grid。
     * <p>
     * 复刻显式排除 Spring 入口（ADR 0001）。
     *
     * @param springCfgUrl Spring XML 配置文件 URL（不可为 {@code null}）。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛——复刻范围不含 Spring。
     */
    public static Ignite start(URL springCfgUrl) throws IgniteException {
        throw new UnsupportedOperationException("Spring XML configuration entry points are out of the replica "
            + "scope (ADR-0001): use Ignition.start(IgniteConfiguration) instead.");
    }

    /**
     * 以 Spring XML 配置输入流启动 grid。
     * <p>
     * 复刻显式排除 Spring 入口（ADR 0001）。
     *
     * @param springCfgStream Spring XML 配置输入流（不可为 {@code null}）。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛——复刻范围不含 Spring。
     */
    public static Ignite start(InputStream springCfgStream) throws IgniteException {
        throw new UnsupportedOperationException("Spring XML configuration entry points are out of the replica "
            + "scope (ADR-0001): use Ignition.start(IgniteConfiguration) instead.");
    }

    /**
     * 取或启动 grid 实例：未启动则启动，已启动则幂等返回既有实例（不抛异常）。
     *
     * @param cfg grid 配置（不可为 {@code null}）。
     * @return grid 实例。
     * @throws IgniteException 启动失败时抛出。
     */
    public static Ignite getOrStart(IgniteConfiguration cfg) throws IgniteException {
        try {
            return IgnitionEx.start(cfg, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * 从 Spring XML 配置按名字加载 bean。
     * <p>
     * 复刻显式排除 Spring 入口（ADR 0001）。
     *
     * @param springXmlPath Spring XML 配置文件路径。
     * @param beanName bean 名。
     * @param <T> bean 类型。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛——复刻范围不含 Spring。
     */
    public static <T> T loadSpringBean(String springXmlPath, String beanName) throws IgniteException {
        throw unsupportedSpring();
    }

    /**
     * 从 Spring XML 配置 URL 按名字加载 bean（复刻排除，见 {@link #loadSpringBean(String, String)}）。
     *
     * @param springXmlUrl Spring XML 配置文件 URL。
     * @param beanName bean 名。
     * @param <T> bean 类型。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛。
     */
    public static <T> T loadSpringBean(URL springXmlUrl, String beanName) throws IgniteException {
        throw unsupportedSpring();
    }

    /**
     * 从 Spring XML 输入流按名字加载 bean（复刻排除，见 {@link #loadSpringBean(String, String)}）。
     *
     * @param springXmlStream Spring XML 配置输入流。
     * @param beanName bean 名。
     * @param <T> bean 类型。
     * @return 不会正常返回。
     * @throws UnsupportedOperationException 恒抛。
     */
    public static <T> T loadSpringBean(InputStream springXmlStream, String beanName) throws IgniteException {
        throw unsupportedSpring();
    }

    /**
     * 取默认（无名）实例。本方法不保证每次返回同一实例引用。
     *
     * @return 默认实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例未启动或已停止时抛出。
     */
    public static Ignite ignite() throws IgniteIllegalStateException {
        return IgnitionEx.grid();
    }

    /**
     * 取 JVM 内全部已启动实例。
     *
     * @return 全部已启动实例的列表。
     */
    public static List<Ignite> allGrids() {
        return IgnitionEx.allGrids();
    }

    /**
     * 按本地节点 ID 取实例（实例与节点一一对应）。
     *
     * @param locNodeId 本地节点 ID。
     * @return 管理该节点的实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例未启动或已停止时抛出。
     */
    public static Ignite ignite(UUID locNodeId) throws IgniteIllegalStateException {
        return IgnitionEx.grid(locNodeId);
    }

    /**
     * 按名字取实例；{@code null} 或空名取默认实例。
     *
     * @param name 实例名（可为 {@code null}）。
     * @return 命名实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例未启动或已停止时抛出。
     */
    public static Ignite ignite(@Nullable String name) throws IgniteIllegalStateException {
        return IgnitionEx.grid(name);
    }

    /**
     * 从闭包内部寻址本地 {@link Ignite} 实例。
     * <p>
     * 依赖 IgniteThread 基建（按约定仅在 IgniteThread 下调用），随章 3 通信线程模型接入。
     *
     * @return 当前线程关联的实例。
     * @throws UnsupportedOperationException 恒抛——IgniteThread 基建未复刻（章 3 接入）。
     */
    public static Ignite localIgnite() throws IgniteIllegalStateException, IllegalArgumentException {
        throw new UnsupportedOperationException("IgniteThread infrastructure is not replicated yet "
            + "(arrives with the communication thread model in Chapter 3).");
    }

    /**
     * 注册实例生命周期监听器；重复注册为无操作。
     * <p>
     * 与其他监听器不同，本监听器由触发状态变更的线程同步回调——保持轻量、自行捕获异常。
     *
     * @param lsnr 监听器。
     */
    public static void addListener(IgnitionListener lsnr) {
        IgnitionEx.addListener(lsnr);
    }

    /**
     * 移除 {@link #addListener(IgnitionListener)} 注册的监听器。
     *
     * @param lsnr 监听器。
     * @return 之前确实注册过返回 {@code true}。
     */
    public static boolean removeListener(IgnitionListener lsnr) {
        return IgnitionEx.removeListener(lsnr);
    }

    /**
     * Spring 排除的统一异常工厂（ADR 0001）。
     *
     * @return 待抛出的异常。
     */
    private static UnsupportedOperationException unsupportedSpring() {
        return new UnsupportedOperationException("Spring bean loading is out of the replica scope (ADR-0001).");
    }
}
