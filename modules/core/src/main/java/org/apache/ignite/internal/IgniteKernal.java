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
// Lesson 0.3：kernal 容器本体——gateway 状态机、GridKernalContextImpl 组装、
// GridComponent 先注册后启动编排、最小闭包（marshaller 三件套 + PoolProcessor +
// GridTimeoutProcessor）。启动序列是 vendor §6 时间线的子序列：
//   gateway STOPPED→STARTING（vendor 步 6）
//   → ctx 组装（步 7）
//   → initializeMarshaller（步 9）
//   → P8 PoolProcessor（vendor §5 #8，步 11 内）
//   → P12 GridTimeoutProcessor（vendor §5 #12，步 12 内）
//   → gw.setState(STARTED)（步 26 前半；discovery 最后启动属章 2.2）
//   → 组件 onKernalStart(true) 回调（步 31）
// 新组件接入时必须插到 vendor 时间线对应位置（生长不变量，测试守卫）。
// 构造器差异：vendor 有带 GridSpringResourceContext 的第二构造器——复刻排除
// Spring（ADR 0001）。签名差异：context() 返回 GridKernalContextImpl（vendor 返回
// GridKernalContext，协变收窄——kernal 内部与测试直接用 Impl 的 add()，公共消费者
// 不受影响）。stop 为最小停链（组件逆序 stop + gateway STOPPING→STOPPED 复位，
// 与 vendor 失败路径 stop(true) 同构）；完整停链（onKernalStop 通知、生命周期 bean
// AFTER_NODE_STOP、shutdown hook、注册表摘除联动）是 0.4 的概念簇。

package org.apache.ignite.internal;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;

import static org.apache.ignite.internal.GridKernalState.STARTED;
import static org.apache.ignite.internal.GridKernalState.STARTING;

/**
 * Ignite kernal——{@link Ignite} 接口的默认实现。
 * <p>
 * 0.3 起成为组件容器：经 {@link GridKernalContextImpl} 注册并启动全部
 * manager/processor，公共 API 可用性由 gateway 状态机守卫。
 */
public class IgniteKernal implements IgniteEx {
    /** kernal gateway（首次 start 惰性创建）。 */
    private final AtomicReference<GridKernalGateway> gw = new AtomicReference<>();

    /** kernal 上下文（组装后非空）。 */
    private volatile GridKernalContextImpl ctx;

    /** 定稿配置（start 时赋值）。 */
    private volatile IgniteConfiguration cfg;

    /** 实例名。 */
    private String igniteInstanceName;

    /** kernal 日志（带 %instanceName 后缀的代理）。 */
    private GridLoggerProxy log;

    /** 启动完成时间戳。 */
    private long startTime;

    /**
     * 创建 kernal（vendor 签名带 Spring 资源上下文参数，复刻排除 Spring，见类头注释）。
     */
    public IgniteKernal() {
        // 空实现。
    }

    /**
     * 启动 kernal（vendor 全签名）。
     * <p>
     * 序列见类头注释（vendor §6 时间线子序列）。失败时执行最小回滚后重抛。
     *
     * @param cfg 定稿配置（initializeConfiguration 的产物）。
     * @param errHnd 启动失败回调（在回滚前调用）。
     * @param workerRegistry worker 注册表。
     * @param hnd 线程池缺省未捕获异常处理器。
     * @param startTimer 启动分段计时袋。
     * @throws IgniteCheckedException 启动失败时抛出。
     */
    public void start(
        final IgniteConfiguration cfg,
        GridAbsClosure errHnd,
        WorkersRegistry workerRegistry,
        Thread.UncaughtExceptionHandler hnd,
        TimeBag startTimer
    ) throws IgniteCheckedException {
        assert cfg != null;
        assert errHnd != null;
        assert startTimer != null;

        gw.compareAndSet(null, new GridKernalGatewayImpl(cfg.getIgniteInstanceName()));

        GridKernalGateway gw = this.gw.get();

        gw.writeLock();

        try {
            switch (gw.getState()) {
                case STARTED: {
                    U.warn(log, "Grid has already been started (ignored).");

                    return;
                }

                case STARTING: {
                    U.warn(log, "Grid is already in process of being started (ignored).");

                    return;
                }

                case STOPPING: {
                    throw new IgniteCheckedException("Grid is in process of being stopped");
                }

                case STOPPED: {
                    break;
                }

                default:
                    // DISCONNECTED 属 client 断连路径（章 13），此处不可达。
                    assert false : "Unexpected kernal state: " + gw.getState();
            }

            gw.setState(STARTING);
        }
        finally {
            gw.writeUnlock();
        }

        validateCommon(cfg);

        igniteInstanceName = cfg.getIgniteInstanceName();

        this.cfg = cfg;

        log = (GridLoggerProxy)cfg.getGridLogger().getLogger(
            getClass().getName() + (igniteInstanceName != null ? '%' + igniteInstanceName : ""));

        try {
            // vendor 步 7：组装 kernal 上下文（marshaller 上下文/空注册表在此诞生）。
            ctx = new GridKernalContextImpl(this, cfg, gw, workerRegistry, hnd);

            // vendor 步 9：初始化 marshaller（绑定上下文与实例名）。
            initializeMarshaller();

            // vendor 步 11（P8）：线程池 processor。
            startProcessor(new PoolProcessor(ctx));

            // vendor 步 12（P12）：超时 processor。
            startProcessor(new GridTimeoutProcessor(ctx));

            // vendor 步 26 前半：gateway 置 STARTED。
            // 后半（最后启动 discovery manager 加入拓扑）属章 2.2——届时插到本位置之后。
            gw.writeLock();

            try {
                gw.setState(STARTED);
            }
            finally {
                gw.writeUnlock();
            }

            // vendor 步 31：其余组件依次 onKernalStart(active)。
            // （active 语义来自集群状态，0.3 无 discovery，恒 true。）
            for (GridComponent comp : ctx)
                comp.onKernalStart(true);
        }
        catch (Throwable e) {
            if (e instanceof InterruptedException)
                U.warn(log, "Grid startup routine has been interrupted (will rollback).");
            else
                U.error(log, "Got exception while starting (will rollback startup routine).", e);

            errHnd.apply();

            // vendor 同款：失败路径走 stop(true)（本课的最小停链形态）。
            stop0(true);

            if (e instanceof Error)
                throw e;
            else if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;
            else
                throw new IgniteCheckedException(e);
        }

        startTime = U.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Ignite node started ok" + (igniteInstanceName != null ? " (instance name: "
                + igniteInstanceName + ')' : ""));

        startTimer.finishGlobalStage("Kernal start");
    }

    /**
     * 初始化 marshaller：绑定 marshaller 上下文与实例名
     * （vendor initializeMarshaller 全量语义，2.18 的 ctx.marshaller() 固定 BinaryMarshaller）。
     */
    private void initializeMarshaller() {
        ctx.marshaller().setContext(ctx.marshallerContext());

        ctx.marshaller().nodeName(igniteInstanceName);
    }

    /**
     * 注册并启动 processor——先 {@code ctx.add} 后 {@code start()}，
     * 避免"已启动但注册表查不到"的窗口（vendor startProcessor 原语义）。
     *
     * @param proc processor。
     * @throws IgniteCheckedException 启动失败时抛出。
     */
    private void startProcessor(GridProcessor proc) throws IgniteCheckedException {
        ctx.add(proc);

        try {
            proc.start();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to start processor: " + proc, e);
        }
    }

    /**
     * 校验公共配置（vendor validateCommon 子集：nodeId 与 logger；
     * SPI 非空/网络超时为正随章 2 SPI 框架接入）。
     *
     * @param cfg 配置。
     * @throws IgniteCheckedException 校验失败时抛出。
     */
    private void validateCommon(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (cfg.getNodeId() == null)
            throw new IgniteCheckedException("Node ID cannot be null.");

        if (cfg.getGridLogger() == null)
            throw new IgniteCheckedException("Grid logger cannot be null.");
    }

    /**
     * 最小停链：gateway STOPPING → 逆序 stop 已注册组件 → 复位 STOPPED。
     * <p>
     * 启动失败路径（catch 内调 {@code stop0(true)}）与注册表侧正式停止共用本链，
     * 保证半启动组件得到清理、后台 worker 线程（如 grid-timeout-worker）不泄漏。
     * 对半启动组件（start 中途抛出者）要求其 stop 幂等。
     * vendor 的完整停链还含组件 onKernalStop 通知、生命周期 bean AFTER_NODE_STOP、
     * worker registry 扫描——属 0.4 的概念簇。
     *
     * @param cancel 是否取消运行中的作业。
     */
    private void stop0(boolean cancel) {
        GridKernalGateway gw = this.gw.get();

        GridKernalContextImpl ctx0 = ctx;

        gw.writeLock();

        try {
            gw.setState(GridKernalState.STOPPING);

            if (ctx0 != null) {
                List<GridComponent> comps = ctx0.components();

                for (int i = comps.size() - 1; i >= 0; i--) {
                    try {
                        comps.get(i).stop(cancel);
                    }
                    catch (Throwable t) {
                        U.error(log, "Failed to stop component: " + comps.get(i), t);
                    }
                }
            }

            ctx = null;

            gw.setState(GridKernalState.STOPPED);
        }
        finally {
            gw.writeUnlock();
        }
    }

    /**
     * 停止 kernal（0.3 为最小停链：组件逆序 stop + gateway STOPPING→STOPPED 复位；
     * onKernalStop 通知、生命周期 bean AFTER_NODE_STOP、shutdown hook 属 0.4）。
     *
     * @param cancel 是否取消运行中的作业。
     */
    public void stop(boolean cancel) {
        assert cfg != null : "Kernal is not started.";

        stop0(cancel);
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

    /** {@inheritDoc} */
    @Override public GridKernalContextImpl context() {
        return ctx;
    }

    /**
     * 取本地节点 ID（读 kernal context；未完成容器组装时回退读配置）。
     *
     * @return 本地节点 ID。
     */
    public UUID localNodeId() {
        assert cfg != null;

        GridKernalContextImpl ctx0 = ctx;

        return ctx0 != null ? ctx0.localNodeId() : cfg.getNodeId();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        Ignition.stop(name(), true);
    }
}
