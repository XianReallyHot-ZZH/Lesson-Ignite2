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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgnitionEx.java
// Lesson 0.2 子集：静态注册表 + 并发/重名/getOrStart 语义 + IgniteNamedInstance 的
// start（startGuard CAS + startLatch 等待）与 initializeConfiguration
//（nodeId 生成 / consistentId 覆盖 / 工作目录解析 / 日志代理包装）。
// start0 / stop / initializeConfiguration 三个方法的"注册表骨架"逐行对齐 vendor，
// 骨架内被掏空的步骤以注释标注归属课程（kernal 容器 0.3、生命周期收口 0.4、
// SPI 章 2、cache 章 1、MBean 13.7……）——即 course-map 的"生长不变量"：
// 复刻启动序列恒为 vendor §6 时间线的子序列。
// 显式差异：Spring 全家（loadConfigurations/start(URL...)/GridSpringResourceContext 参数）不复制，
// 由公共门面 Ignition 抛 UnsupportedOperationException（ADR 0001）。

package org.apache.ignite.internal;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.IgniteState.STOPPED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_RESTART_CODE;
import static org.apache.ignite.IgniteSystemProperties.getString;

/**
 * 本类属于内部 API，可随时变更且不保证兼容。
 * <p>
 * {@link Ignition} 的实现载体：持有 JVM 级实例注册表（{@code grids} / {@code dfltGrid}），
 * 承载重名抛异常、getOrStart 幂等、并发启动互斥等语义；每个实例的启动编排
 * （配置定稿 → kernal 创建 → 状态推进 → 失败回滚）由内部类 {@link IgniteNamedInstance} 完成。
 */
public class IgnitionEx {
    /** 相对 Ignite home 的默认配置路径（Spring 入口专用；复刻排除，仅保留常量）。 */
    public static final String DFLT_CFG = "config/default-config.xml";

    /** 命名实例注册表。 */
    private static final ConcurrentMap<Object, IgniteNamedInstance> grids = new ConcurrentHashMap<>();

    /** 本 JVM 启动过的实例状态表（实例摘除后仍保留最近状态）。 */
    private static final Map<Object, IgniteState> gridStates = new ConcurrentHashMap<>();

    /** 默认实例引用的更新互斥。 */
    private static final Object dfltGridMux = new Object();

    /** 默认实例。 */
    private static volatile IgniteNamedInstance dfltGrid;

    /** 默认实例的最近状态。 */
    private static volatile IgniteState dfltGridState;

    /** 状态监听器集合。 */
    private static final GridConcurrentHashSet<IgnitionListener> lsnrs = new GridConcurrentHashSet<>(4);

    /** client 模式线程局部标志（{@code Ignition.setClientMode}）。 */
    private static final ThreadLocal<Boolean> clientMode = new ThreadLocal<>();

    /**
     * 单例约束。
     */
    private IgnitionEx() {
        // 空实现。
    }

    /**
     * 设置 client 模式标志。
     *
     * @param clientMode client 模式标志。
     */
    public static void setClientMode(boolean clientMode) {
        IgnitionEx.clientMode.set(clientMode);
    }

    /**
     * 取 client 模式标志。
     *
     * @return 标志值（未设置为 {@code false}）。
     */
    public static boolean isClientMode() {
        return clientMode.get() != null && clientMode.get();
    }

    /**
     * 取默认 grid 状态。
     *
     * @return 默认 grid 状态。
     */
    public static IgniteState state() {
        return state(null);
    }

    /**
     * 取命名实例状态；{@code null} 名取默认实例。注册表无此实例时返回其最近状态
     * （从未启动过则为 {@code STOPPED}）。
     *
     * @param name 实例名（可为 {@code null}）。
     * @return 实例状态。
     */
    public static IgniteState state(@Nullable String name) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        if (grid == null) {
            IgniteState state = name != null ? gridStates.get(name) : dfltGridState;

            return state != null ? state : STOPPED;
        }

        return grid.state();
    }

    /**
     * 停止默认 grid。
     *
     * @param cancel 是否取消运行中的作业。
     * @param shutdown 停机策略（{@code null} 表示按集群策略）。
     * @return 确实停止了返回 {@code true}；本就未启动返回 {@code false}。
     */
    public static boolean stop(boolean cancel, @Nullable ShutdownPolicy shutdown) {
        return stop(null, cancel, shutdown, false);
    }

    /**
     * 停止命名实例。注册表操作骨架与 vendor 一致：等待在途启动（需要时打断启动线程）、
     * 停止实例、从注册表摘除、通知状态监听器。
     *
     * @param name 实例名（{@code null} 停默认实例）。
     * @param cancel 是否取消运行中的作业。
     * @param shutdown 停机策略。
     * @param stopNotStarted 启动未完成时是否打断启动线程。
     * @return 找到并停止了返回 {@code true}；实例不存在返回 {@code false}。
     */
    public static boolean stop(@Nullable String name, boolean cancel,
        @Nullable ShutdownPolicy shutdown, boolean stopNotStarted) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        if (grid != null && stopNotStarted && grid.startLatch.getCount() != 0) {
            grid.starterThreadInterrupted = true;

            grid.starterThread.interrupt();
        }

        if (grid != null) {
            if (grid.state() == STARTED)
                grid.stop(cancel, shutdown);

            boolean fireEvt;

            if (name != null)
                fireEvt = grids.remove(name, grid);
            else {
                synchronized (dfltGridMux) {
                    fireEvt = dfltGrid == grid;

                    if (fireEvt)
                        dfltGrid = null;
                }
            }

            if (fireEvt)
                notifyStateChange(grid.getName(), grid.state());

            return true;
        }

        // 到这里说明实例已停或从未启动，没有日志可用。
        U.warn(null, "Ignoring stopping Ignite instance that was already stopped or never started: " + name);

        return false;
    }

    /**
     * 停止当前 JVM 内全部 grid（默认实例优先，其余按注册表遍历）。
     *
     * @param cancel 是否取消运行中的作业。
     * @param shutdown 停机策略。
     */
    public static void stopAll(boolean cancel, @Nullable ShutdownPolicy shutdown) {
        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            dfltGrid0.stop(cancel, shutdown);

            boolean fireEvt;

            synchronized (dfltGridMux) {
                fireEvt = dfltGrid == dfltGrid0;

                if (fireEvt)
                    dfltGrid = null;
            }

            if (fireEvt)
                notifyStateChange(dfltGrid0.getName(), dfltGrid0.state());
        }

        // 停掉其余命名实例并清空注册表。
        for (IgniteNamedInstance grid : grids.values()) {
            grid.stop(cancel, shutdown);

            boolean fireEvt = grids.remove(grid.getName(), grid);

            if (fireEvt)
                notifyStateChange(grid.getName(), grid.state());
        }
    }

    /**
     * 重启全部 grid：写重启标记文件后停止全部实例，再以重启退出码退出 JVM，
     * 由外部脚本（ignite.sh/bat）拉起新进程。
     *
     * @param cancel 是否取消运行中的作业。
     */
    public static void restart(boolean cancel) {
        String file = getString(IgniteSystemProperties.IGNITE_SUCCESS_FILE);

        if (file == null)
            U.warn(null, "Cannot restart node when restart not enabled.");
        else {
            try {
                new File(file).createNewFile();
            }
            catch (IOException e) {
                U.warn(null, "Failed to create restart marker file (restart aborted): " + e.getMessage());

                return;
            }

            // 设置退出码供 shell 识别并循环启动序列。
            System.setProperty(IGNITE_RESTART_CODE, Integer.toString(Ignition.RESTART_EXIT_CODE));

            stopAll(cancel, null);

            // 放弃可能挂起的加载器线程——直接退出。
            System.exit(Ignition.RESTART_EXIT_CODE);
        }
    }

    /**
     * 停止全部 grid 后以 KILL_EXIT_CODE 强制退出 JVM。
     *
     * @param cancel 是否取消运行中的作业。
     */
    public static void kill(boolean cancel) {
        stopAll(cancel, null);

        // 放弃可能挂起的加载器线程——直接退出。
        System.exit(Ignition.KILL_EXIT_CODE);
    }

    /**
     * 以默认配置启动 grid（vendor 会先找 IGNITE_HOME 下的默认 Spring 配置文件；
     * 复刻排除 Spring（ADR 0001），直接以全默认 {@link IgniteConfiguration} 启动）。
     *
     * @return 已启动的 grid。
     * @throws IgniteCheckedException 启动失败或默认实例已启动时抛出。
     */
    public static Ignite start() throws IgniteCheckedException {
        return start0(new GridStartContext(new IgniteConfiguration(), null), true).get1().grid();
    }

    /**
     * 以给定配置启动 grid；同名实例已启动则抛异常。
     *
     * @param cfg grid 配置（不可为 {@code null}）。
     * @return 已启动的 grid。
     * @throws IgniteCheckedException 启动失败或同名实例已启动时抛出。
     */
    public static Ignite start(IgniteConfiguration cfg) throws IgniteCheckedException {
        return start(cfg, true);
    }

    /**
     * 以给定配置启动 grid。已启动场景按 {@code failIfStarted} 分流：
     * {@code true} 抛异常（start 语义），{@code false} 幂等返回既有实例（getOrStart 语义）。
     *
     * @param cfg grid 配置（不可为 {@code null}）。
     * @param failIfStarted 已启动时是否抛异常。
     * @return 已启动的 grid 或既有 grid。
     * @throws IgniteCheckedException 启动失败（或已启动且 failIfStarted）时抛出。
     */
    public static Ignite start(IgniteConfiguration cfg, boolean failIfStarted) throws IgniteCheckedException {
        A.notNull(cfg, "cfg");

        return start0(new GridStartContext(cfg, null), failIfStarted).get1().grid();
    }

    /**
     * 取或启动 grid 实例，附带"本次调用是否真的启动了它"标志。
     *
     * @param cfg grid 配置（不可为 {@code null}）。
     * @return 二元组：grid 实例 + 本次调用是否启动（既有实例则为 {@code false}）。
     * @throws org.apache.ignite.IgniteException 启动失败时抛出。
     */
    public static T2<Ignite, Boolean> getOrStart(IgniteConfiguration cfg) throws org.apache.ignite.IgniteException {
        try {
            T2<IgniteNamedInstance, Boolean> res = start0(new GridStartContext(cfg, null), false);

            return new T2<>(res.get1().grid(), res.get2());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * 注册表语义核心（逐行对齐 vendor start0）：
     * <ol>
     *   <li>空名拒绝：命名实例不允许空字符串名；</li>
     *   <li>占位登记：putIfAbsent（命名）/ dfltGridMux 互斥（默认）；</li>
     *   <li>竞争仲裁：对手"已停未摘"则原子替换，替换失败抛并发启动异常；对手已启动则按
     *       failIfStarted 抛 already-started 或幂等返回既有实例；</li>
     *   <li>启动编排：IgniteNamedInstance.start；成功后 notifyStateChange(STARTED)，
     *       失败从注册表摘除（finally 兜底）。</li>
     * </ol>
     * vendor 另在此处执行 warmupClosure（配置预热闭包）——配置子集未含该字段，随部署课补。
     *
     * @param startCtx 启动上下文。
     * @param failIfStarted 已启动时是否抛异常。
     * @return 二元组：命名实例 + 本次是否启动。
     * @throws IgniteCheckedException 启动失败时抛出。
     */
    private static T2<IgniteNamedInstance, Boolean> start0(
        GridStartContext startCtx,
        boolean failIfStarted
    ) throws IgniteCheckedException {
        assert startCtx != null;

        String name = startCtx.config().getIgniteInstanceName();

        if (name != null && name.isEmpty())
            throw new IgniteCheckedException("Non default Ignite instances cannot have empty string name.");

        IgniteNamedInstance grid = new IgniteNamedInstance(name);

        IgniteNamedInstance old;

        if (name != null)
            old = grids.putIfAbsent(name, grid);
        else {
            synchronized (dfltGridMux) {
                old = dfltGrid;

                if (old == null)
                    dfltGrid = grid;
            }
        }

        if (old != null)
            if (old.grid() == null) { // 已停止但尚未从注册表摘除。
                boolean replaced;

                if (name != null)
                    replaced = grids.replace(name, old, grid);
                else {
                    synchronized (dfltGridMux) {
                        replaced = old == dfltGrid;

                        if (replaced)
                            dfltGrid = grid;
                    }
                }

                if (!replaced) {
                    throw new IgniteCheckedException("Ignite instance with this name has been concurrently started: " +
                        name);
                }
                else
                    notifyStateChange(old.getName(), old.state());
            }
            else if (failIfStarted) {
                if (name == null)
                    throw new IgniteCheckedException("Default Ignite instance has already been started.");
                else
                    throw new IgniteCheckedException("Ignite instance with this name has already been started: " +
                        name);
            }
            else
                return new T2<>(old, false);

        startCtx.single(grids.size() == 1);

        boolean success = false;

        try {
            try {
                grid.start(startCtx);
            }
            catch (Exception e) {
                if (X.hasCause(e, IgniteInterruptedCheckedException.class, InterruptedException.class)) {
                    if (grid.starterThreadInterrupted)
                        Thread.interrupted();
                }

                throw e;
            }

            notifyStateChange(name, STARTED);

            success = true;
        }
        finally {
            if (!success) {
                if (name != null)
                    grids.remove(name, grid);
                else {
                    synchronized (dfltGridMux) {
                        if (dfltGrid == grid)
                            dfltGrid = null;
                    }
                }

                grid = null;
            }
        }

        if (grid == null)
            throw new IgniteCheckedException("Failed to start grid with provided configuration.");

        return new T2<>(grid, true);
    }

    /**
     * 取默认（无名）实例。
     *
     * @return 默认实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例不存在或已停止时抛出。
     */
    public static Ignite grid() throws IgniteIllegalStateException {
        return grid((String)null);
    }

    /**
     * 按名字取实例（等待其初始化完成后返回）。
     *
     * @param name 实例名（{@code null} 取默认实例）。
     * @return 实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例不存在或已停止时抛出。
     */
    public static Ignite grid(@Nullable String name) throws IgniteIllegalStateException {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        Ignite res;

        if (grid == null || (res = grid.grid()) == null)
            throw new IgniteIllegalStateException("Ignite instance with provided name doesn't exist. " +
                "Did you call Ignition.start(..) to start an Ignite instance? [name=" + name + ']');

        return res;
    }

    /**
     * 按本地节点 ID 取实例（实例与节点一一对应；等待实例初始化定局）。
     *
     * @param locNodeId 本地节点 ID。
     * @return 实例（永不返回 {@code null}）。
     * @throws IgniteIllegalStateException 实例不存在或已停止时抛出。
     */
    public static Ignite grid(UUID locNodeId) throws IgniteIllegalStateException {
        A.notNull(locNodeId, "locNodeId");

        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            IgniteKernal g = dfltGrid0.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        for (IgniteNamedInstance grid : grids.values()) {
            IgniteKernal g = grid.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        throw new IgniteIllegalStateException("Grid instance with given local node ID was not properly " +
            "started or was stopped: " + locNodeId);
    }

    /**
     * 取全部已启动实例（等待各自初始化完成；默认实例排最后——vendor 顺序）。
     *
     * @return 实例列表。
     */
    public static List<Ignite> allGrids() {
        return allGrids(true);
    }

    /**
     * 取全部已启动实例（不等待初始化）。
     *
     * @return 实例列表。
     */
    public static List<Ignite> allGridsx() {
        return allGrids(false);
    }

    /**
     * 取全部已启动实例的实现。
     *
     * @param wait {@code true} 时等待实例启动完成。
     * @return 实例列表。
     */
    private static List<Ignite> allGrids(boolean wait) {
        List<Ignite> allIgnites = new ArrayList<>(grids.size() + 1);

        for (IgniteNamedInstance grid : grids.values()) {
            Ignite g = wait ? grid.grid() : grid.gridx();

            if (g != null)
                allIgnites.add(g);
        }

        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            Ignite g = wait ? dfltGrid0.grid() : dfltGrid0.gridx();

            if (g != null)
                allIgnites.add(g);
        }

        return allIgnites;
    }

    /**
     * 注册状态监听器（重复添加为无操作）。
     *
     * @param lsnr 监听器。
     */
    public static void addListener(IgnitionListener lsnr) {
        A.notNull(lsnr, "lsnr");

        lsnrs.add(lsnr);
    }

    /**
     * 移除状态监听器。
     *
     * @param lsnr 监听器。
     * @return 之前确实注册过返回 {@code true}。
     */
    public static boolean removeListener(IgnitionListener lsnr) {
        A.notNull(lsnr, "lsnr");

        return lsnrs.remove(lsnr);
    }

    /**
     * 更新实例状态表并同步通知全部监听器。
     *
     * @param igniteInstanceName 实例名（默认实例为 {@code null}）。
     * @param state 工厂侧状态。
     */
    private static void notifyStateChange(@Nullable String igniteInstanceName, IgniteState state) {
        if (igniteInstanceName != null)
            gridStates.put(igniteInstanceName, state);
        else
            dfltGridState = state;

        for (IgnitionListener lsnr : lsnrs)
            lsnr.onStateChange(igniteInstanceName, state);
    }

    /**
     * kernal 是否已完全启动（startLatch 计数归零）。
     *
     * @param name 实例名（默认实例为 {@code null}）。
     * @return 全部组件启动完成返回 {@code true}。
     */
    public static boolean hasKernalStarted(String name) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        return grid != null && grid.hasStartLatchCompleted();
    }

    /**
     * 启动上下文：携带启动参数穿过注册表层进入实例层。
     * vendor 版本另含 Spring 资源上下文字段（复刻排除，ADR 0001）。
     */
    private static final class GridStartContext {
        /** 用户配置。 */
        private IgniteConfiguration cfg;

        /** 可选配置文件 URL。 */
        private URL cfgUrl;

        /** 本 JVM 内是否只有这一个 grid 实例。 */
        private boolean single;

        /**
         * @param cfg 用户配置。
         * @param cfgUrl 可选配置文件 URL。
         */
        GridStartContext(IgniteConfiguration cfg, @Nullable URL cfgUrl) {
            assert cfg != null;

            this.cfg = cfg;
            this.cfgUrl = cfgUrl;
        }

        /**
         * @return 是否单实例 JVM。
         */
        public boolean single() {
            return single;
        }

        /**
         * @param single 是否单实例 JVM。
         */
        public void single(boolean single) {
            this.single = single;
        }

        /**
         * @return 用户配置。
         */
        IgniteConfiguration config() {
            return cfg;
        }

        /**
         * @return 可选配置文件 URL。
         */
        URL configUrl() {
            return cfgUrl;
        }
    }

    /**
     * 命名实例容器：一个实例的配置定稿、kernal 生命周期与实例级状态。
     */
    private static final class IgniteNamedInstance {
        /** 实例名（默认实例为 {@code null}）。 */
        private final String name;

        /** kernal 实例（启动中到停止前非空）。 */
        private volatile IgniteKernal grid;

        /** 实例状态。 */
        private volatile IgniteState state = STOPPED;

        /** 工厂日志。 */
        private IgniteLogger log;

        /** 启动防重入闸门（CAS 只允许一次 start）。 */
        private final AtomicBoolean startGuard = new AtomicBoolean();

        /** 启动完成闩：grid()/state() 在非启动线程上等待它。 */
        private final CountDownLatch startLatch = new CountDownLatch(1);

        /**
         * 启动本实例的线程（仅在启动线程自身上下文有意义，无需 volatile）。
         */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private Thread starterThread;

        /** 启动线程是否被 stopNotStarted 打断。 */
        private boolean starterThreadInterrupted;

        /**
         * 创建未启动的命名实例。
         *
         * @param name 实例名（默认实例为 {@code null}）。
         */
        IgniteNamedInstance(@Nullable String name) {
            this.name = name;
        }

        /**
         * 取实例名。
         *
         * @return 实例名。
         */
        String getName() {
            return name;
        }

        /**
         * 取 kernal：非启动线程会等待启动完成（startLatch），
         * 保证注册表竞争者读到的是启动已定局的结果。
         *
         * @return kernal（已停止为 {@code null}）。
         */
        IgniteKernal grid() {
            if (starterThread != Thread.currentThread())
                U.awaitQuiet(startLatch);

            return grid;
        }

        /**
         * 取 kernal 不等待初始化。
         *
         * @return kernal（未就绪或已停止为 {@code null}）。
         */
        public IgniteKernal gridx() {
            return grid;
        }

        /**
         * 取实例状态（非启动线程等待启动定局，见 {@link #grid()}）。
         *
         * @return 实例状态。
         */
        IgniteState state() {
            if (starterThread != Thread.currentThread())
                U.awaitQuiet(startLatch);

            return state;
        }

        /**
         * 启动实例（synchronized 与 startGuard 双重防护：并发第二次调用只等待不重入）。
         *
         * @param startCtx 启动上下文。
         * @throws IgniteCheckedException 启动失败时抛出。
         */
        synchronized void start(GridStartContext startCtx) throws IgniteCheckedException {
            if (startGuard.compareAndSet(false, true)) {
                try {
                    starterThread = Thread.currentThread();

                    IgniteConfiguration myCfg = initializeConfiguration(
                        startCtx.config() != null ? startCtx.config() : new IgniteConfiguration()
                    );

                    // vendor：TimeBag 分段计时并打 "Node started : [...]"；
                    // startTimer 随 0.3 的 vendor 全签名 start 接入，这里保底一行。
                    start0(startCtx, myCfg);

                    if (log.isInfoEnabled())
                        log.info("Node started : " + name);
                }
                finally {
                    startLatch.countDown();
                }
            }
            else
                U.awaitQuiet(startLatch);
        }

        /**
         * 实例级启动编排。vendor 版本在此（按序）：SPI 多实例注解校验（章 2）、
         * WorkersRegistry/OOM handler（0.3+ 故障处理）、factory MBean 注册（13.7）、
         * kernal 全签名 start（0.3）、JVM shutdown hook 安装（0.4）。
         *
         * @param startCtx 启动上下文。
         * @param cfg 定稿配置。
         * @throws IgniteCheckedException 启动失败时抛出。
         */
        private void start0(GridStartContext startCtx, IgniteConfiguration cfg) throws IgniteCheckedException {
            assert grid == null : "Grid is already started: " + name;

            // 配置文件 URL 存在时写入系统属性（Spring 入口遗留记录点）。
            if (startCtx.configUrl() != null)
                System.setProperty(IGNITE_CONFIG_URL, startCtx.configUrl().toString());

            boolean started = false;

            try {
                IgniteKernal grid0 = new IgniteKernal();

                // 先赋值再 start：让生命周期监听者能在启动期看到 grid。
                grid = grid0;

                grid0.start(cfg);

                state = STARTED;

                if (log.isDebugEnabled())
                    log.debug("Grid factory started ok: " + name);

                started = true;
            }
            catch (IgniteCheckedException e) {
                throw e;
            }
            // 兜住一切可能的失败，保护 JVM 不被半启动实例污染。
            catch (Throwable e) {
                if (e instanceof Error)
                    throw e;

                throw new IgniteCheckedException("Unexpected exception when starting grid.", e);
            }
            finally {
                if (!started)
                    // 启动失败，kernal 不可用。
                    grid = null;
            }
        }

        /**
         * 停止实例。策略为 {@code null} 时取默认：vendor 先看
         * IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN 系统属性（GRACEFUL 立即生效开关），
         * 否则读集群 metastore 的集群级策略；复刻在 metastorage 落地（章 7）前恒为 IMMEDIATE。
         *
         * @param cancel 是否取消运行中的作业。
         * @param shutdown 停机策略。
         */
        void stop(boolean cancel, ShutdownPolicy shutdown) {
            // stop 不可能先于 start 从公共 API 到达（公共入口检查 STARTED 状态），可断言。
            assert startGuard.get();

            if (shutdown == null)
                shutdown = ShutdownPolicy.IMMEDIATE;

            stop0(cancel, shutdown);
        }

        /**
         * 实例级停止编排。vendor 版本在此（按序）：摘除 JVM shutdown hook（0.4）、
         * GRACEFUL 等备份循环（章 7 metastore/cache）、grid0.stop(cancel) 组件逆序停链（0.4）、
         * 分段/失败终态（STOPPED_ON_SEGMENTATION/STOPPED_ON_FAILURE，需 kernal context）。
         *
         * @param cancel 是否取消运行中的作业。
         * @param shutdown 停机策略。
         */
        private synchronized void stop0(boolean cancel, ShutdownPolicy shutdown) {
            IgniteKernal grid0 = grid;

            // 双重检查：可能已被并发 stop。
            if (grid0 == null) {
                if (log != null)
                    U.warn(log, "Attempting to stop an already stopped Ignite instance (ignore): " + name);

                return;
            }

            try {
                grid0.stop(cancel);

                if (log != null && log.isDebugEnabled())
                    log.debug("Ignite instance stopped ok: " + name);
            }
            catch (Throwable e) {
                U.warn(log, "Failed to properly stop grid instance due to undeclared exception: " + e);

                if (e instanceof Error)
                    throw e;
            }
            finally {
                // vendor：依 kernal context 的 segmented()/invalid() 分流
                // STOPPED_ON_SEGMENTATION / STOPPED_ON_FAILURE；context 属 0.3。
                state = STOPPED;

                grid = null;

                log = null;
            }
        }

        /**
         * 配置定稿（0.2 概念簇之一）。对齐 vendor initializeConfiguration 的骨架，
         * 完成的步骤：配置拷贝、ignite home 解析、工作目录解析、nodeId 生成、
         * consistentId 覆盖、日志初始化与代理包装、用户属性补默认。
         * 未接入步骤以注释标明归属课程（见方法尾部汇总注释）。
         *
         * @param cfg 用户配置。
         * @return 定稿后的新配置（用户原配置不被改动）。
         * @throws IgniteCheckedException 定稿失败（非法 home 等）时抛出。
         */
        private IgniteConfiguration initializeConfiguration(IgniteConfiguration cfg)
            throws IgniteCheckedException {
            // vendor 首行：BinaryUtils.initUseBinaryArrays()——binary 数组开关探测，
            // 随 binary 模块课（章 3.10）接入。

            IgniteConfiguration myCfg = new IgniteConfiguration(cfg);

            String ggHome = cfg.getIgniteHome();

            // 解析 Ignite home。
            if (ggHome == null)
                ggHome = U.getIgniteHome();
            else
                // 用户显式提供 home 时同步为系统属性。
                U.setIgniteHome(ggHome);

            String userProvidedWorkDir = cfg.getWorkDirectory();

            // 解析工作目录并写回配置。
            String workDir = U.workDirectory(userProvidedWorkDir, ggHome);

            myCfg.setWorkDirectory(workDir);

            // 不变量：实例名在注册表与配置中一致（略脏但是晚近重构的结果——vendor 原注释）。
            assert Objects.equals(name, cfg.getIgniteInstanceName());

            UUID nodeId = cfg.getNodeId() != null ? cfg.getNodeId() : UUID.randomUUID();

            myCfg.setNodeId(nodeId);

            String predefineConsistentId = getString(IGNITE_OVERRIDE_CONSISTENT_ID);

            if (predefineConsistentId != null && !predefineConsistentId.isEmpty())
                myCfg.setConsistentId(predefineConsistentId);

            IgniteLogger cfgLog = U.initLogger(cfg.getGridLogger(), null, nodeId, workDir);

            assert cfgLog != null;

            cfgLog = new GridLoggerProxy(cfgLog, null, name, U.id8(nodeId));

            // 初始化工厂日志。
            log = cfgLog.getLogger(G.class);

            myCfg.setGridLogger(cfgLog);

            if ((userProvidedWorkDir == null || userProvidedWorkDir.isEmpty()) && U.IGNITE_WORK_DIR == null)
                log.warning("Ignite work directory is not provided, automatically resolved to: " + workDir);

            // 日志可用后校验 home 目录。
            if (ggHome != null) {
                File ggHomeFile = new File(ggHome);

                if (!ggHomeFile.exists() || !ggHomeFile.isDirectory())
                    throw new IgniteCheckedException("Invalid Ignite installation home folder: " + ggHome);
            }

            myCfg.setIgniteHome(ggHome);

            if (myCfg.getUserAttributes() == null)
                myCfg.setUserAttributes(Collections.emptyMap());

            // vendor 此后还有（按归属课程排列，接入时按 vendor 时间线插回）：
            // - segmentation 配置校验与告警（章 2）；
            // - 持久化场景 consistentId 未设告警（章 7）；
            // - TransactionConfiguration/ConnectorConfiguration 拷贝（章 6/13）；
            // - localHost/clientMode/deployment 覆盖（章 2/4/12）；
            // - initializeDefaultMBeanServer（13.7 JMX 收口）；
            // - initializeDefaultSpi：十二个默认 SPI 注入（章 2 SPI 框架）；
            // - GridDiscoveryManager.initCommunicationErrorResolveConfiguration（章 3）；
            // - initializeDefaultCacheConfiguration：utility cache 追加与保留名校验（章 1）；
            // - executor 配置拷贝（章 11）；
            // - initializeDataStorageConfiguration（章 7）。

            return myCfg;
        }

        /**
         * @return startLatch 是否已归零（kernal 完全启动）。
         */
        public boolean hasStartLatchCompleted() {
            return startLatch.getCount() == 0;
        }
    }
}
