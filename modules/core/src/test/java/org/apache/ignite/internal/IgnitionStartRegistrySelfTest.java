/*
 * Lesson-Ignite2 自写红绿切片（vendor 无单一对应物，语义来源为 vendor
 * IgnitionEx.start0 / IgniteNamedInstance.initializeConfiguration / stop 的注册表骨架，
 * 引用见各用例注释；类名沿用 vendor "SelfTest" 后缀惯例）。
 */

package org.apache.ignite.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Test;

/**
 * {@link IgnitionEx} 实例注册表语义与 {@code initializeConfiguration} 定稿步骤的测试。
 */
public class IgnitionStartRegistrySelfTest {
    /**
     * 停掉 JVM 内全部实例，并复位 home 缓存（failed-start 用例会写入坏 home）。
     */
    @After
    public void tearDown() {
        Ignition.stopAll(true);

        System.clearProperty(IgniteSystemProperties.IGNITE_HOME);
        System.clearProperty(IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID);

        U.setIgniteHome(null);
    }

    /**
     * 构造静音测试配置。
     *
     * @param name 实例名。
     * @return 配置。
     */
    private IgniteConfiguration config(String name) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    /**
     * start 后：state(STARTED)、ignite(name) 同实例、allGrids 含本实例
     * （vendor start0 的 putIfAbsent 登记 + notifyStateChange(STARTED)，IgnitionEx.java:1033/1096）。
     */
    @Test
    public void startRegistersInstanceAndState() {
        try (Ignite ignite = Ignition.start(config("registry-1"))) {
            assertEquals("registry-1", ignite.name());
            assertEquals(IgniteState.STARTED, Ignition.state("registry-1"));
            assertSame(ignite, Ignition.ignite("registry-1"));

            assertTrue(Ignition.allGrids().contains(ignite));
        }
    }

    /**
     * 配置未给 nodeId 时启动期生成随机 UUID（vendor IgnitionEx.java:1822-1824），
     * 且两个实例的 nodeId 互不相同。
     */
    @Test
    public void nodeIdGeneratedWhenAbsent() {
        try (Ignite a = Ignition.start(config("nodeid-gen-a"));
             Ignite b = Ignition.start(config("nodeid-gen-b"))
        ) {
            UUID idA = a.configuration().getNodeId();
            UUID idB = b.configuration().getNodeId();

            assertNull(new IgniteConfiguration().setIgniteInstanceName("x").getNodeId());

            assertTrue(idA != null && idB != null);
            assertNotSame(idA, idB);
        }
    }

    /**
     * 配置显式给了 nodeId 则原样保留（vendor 同一行三元表达式的左支）。
     */
    @Test
    public void nodeIdPreservedWhenConfigured() {
        UUID fixed = UUID.randomUUID();

        IgniteConfiguration cfg = config("nodeid-fixed");
        cfg.setNodeId(fixed);

        try (Ignite ignite = Ignition.start(cfg)) {
            assertEquals(fixed, ignite.configuration().getNodeId());
        }
    }

    /**
     * IGNITE_OVERRIDE_CONSISTENT_ID 系统属性覆盖配置的 consistentId
     * （vendor IgnitionEx.java:1826-1829）；未设置时保持原值。
     */
    @Test
    public void consistentIdOverriddenBySystemProperty() {
        System.setProperty(IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID, "override-cid");

        try (Ignite ignite = Ignition.start(config("consistent-id"))) {
            assertEquals("override-cid", ignite.configuration().getConsistentId());
        }

        System.clearProperty(IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID);

        try (Ignite ignite = Ignition.start(config("consistent-id-2"))) {
            assertNull(ignite.configuration().getConsistentId());
        }
    }

    /**
     * 用户配置的工作目录原样保留（vendor setWorkDirectory 回写，IgnitionEx.java:1815）。
     */
    @Test
    public void workDirectoryKeptWhenConfigured() {
        File tmp = new File(System.getProperty("java.io.tmpdir"), "ignite-work-registry-" + UUID.randomUUID());

        IgniteConfiguration cfg = config("workdir");
        cfg.setWorkDirectory(tmp.getAbsolutePath());

        try (Ignite ignite = Ignition.start(cfg)) {
            assertEquals(tmp.getAbsolutePath(), ignite.configuration().getWorkDirectory());
            assertTrue(tmp.isDirectory());
        }
    }

    /**
     * 未配置工作目录时自动解析为绝对路径并告警一次
     * （vendor U.workDirectory 的 user.dir/ignite/work 兜底分支
     * + IgnitionEx.java:1842-1843 的 "automatically resolved to" 告警）。
     */
    @Test
    public void workDirectoryAutoResolvedToAbsolutePath() {
        RecordingLog log = new RecordingLog();

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("workdir-auto");
        cfg.setGridLogger(log);

        try (Ignite ignite = Ignition.start(cfg)) {
            assertTrue(new File(ignite.configuration().getWorkDirectory()).isAbsolute());
        }

        boolean warned = false;

        for (String msg : log.msgs)
            if (msg.contains("Ignite work directory is not provided, automatically resolved to:"))
                warned = true;

        assertTrue("Expected work directory auto-resolution warning", warned);
    }

    /**
     * 用户 logger 被 GridLoggerProxy 包装进定稿配置，且代理逐级委托
     * （vendor IgnitionEx.java:1831-1840：initLogger → proxy → getLogger(G.class)）。
     */
    @Test
    public void gridLoggerWrappedInProxyAndDelegates() {
        RecordingLog log = new RecordingLog();
        log.fileName = "test-ignite.log";

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("logger-proxy");
        cfg.setGridLogger(log);

        try (Ignite ignite = Ignition.start(cfg)) {
            assertTrue(ignite.configuration().getGridLogger() instanceof GridLoggerProxy);

            assertNotSame(log, ignite.configuration().getGridLogger());

            // 公共 API 的 log() 应把消息透传到底层实现（经由代理富化路径）。
            ignite.log().info("hello-through-proxy");

            assertTrue(log.msgs.contains("hello-through-proxy"));
            assertEquals("test-ignite.log", ignite.log().fileName());
        }
    }

    /**
     * 二次同名 start 抛异常，消息与 vendor 逐字一致
     * （"Ignite instance with this name has already been started: ..."，IgnitionEx.java:1070）。
     */
    @Test
    public void secondStartThrowsWithVendorMessage() {
        try (Ignite ignored = Ignition.start(config("dup"))) {
            try {
                Ignition.start(config("dup"));

                fail("Expected IgniteException for duplicate name");
            }
            catch (IgniteException e) {
                assertTrue(e.getMessage().contains(
                    "Ignite instance with this name has already been started: dup"));
            }
        }

        // 默认实例变体（"Default Ignite instance has already been started."）。
        try (Ignite ignored = Ignition.start(config(null))) {
            try {
                Ignition.start(config(null));

                fail("Expected IgniteException for duplicate default instance");
            }
            catch (IgniteException e) {
                assertTrue(e.getMessage().contains("Default Ignite instance has already been started."));
            }
        }
    }

    /**
     * 空字符串实例名被拒绝（vendor IgnitionEx.java:1026-1027）。
     */
    @Test
    public void emptyInstanceNameRejected() {
        try {
            Ignition.start(config(""));

            fail("Expected IgniteException for empty instance name");
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage().contains("Non default Ignite instances cannot have empty string name."));
        }
    }

    /**
     * 并发同名启动恰有一个赢家：其余竞争者等 startLatch 定局后抛 already-started，
     * 且赢家实例被所有拿到引用的线程共享（vendor start0 的 putIfAbsent + grid() 等待语义）。
     */
    @Test
    public void concurrentStartSingleWinner() throws Exception {
        final String name = "concurrent-1";
        final int threads = 8;

        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch go = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);

        final List<Ignite> successes = new CopyOnWriteArrayList<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(new Runnable() {
                    @Override public void run() {
                        ready.countDown();

                        try {
                            go.await();

                            successes.add(Ignition.start(config(name)));
                        }
                        catch (Throwable e) {
                            failures.add(e);
                        }
                    }
                });
            }

            ready.await();
            go.countDown();

            // 等全部线程定局（确保断言发生在竞争结束后）。
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }
        finally {
            pool.shutdownNow();
        }

        assertEquals("Exactly one winner expected", 1, successes.size());
        assertEquals(threads - 1, failures.size());

        for (Throwable t : failures)
            assertTrue(t instanceof IgniteException
                && t.getMessage().contains("Ignite instance with this name has already been started"));

        Ignition.stop(name, true);

        // 竞争过后注册表只剩一个（赢家）实例。
        assertEquals(IgniteState.STOPPED, Ignition.state(name));
        assertEquals(0, Ignition.allGrids().size());
    }

    /**
     * 启动失败（非法 home 目录）时：start 抛异常、注册表槽位被 finally 摘除、
     * 同名重启可成功（vendor start0 的 finally 回滚，IgnitionEx.java:1100-1113；
     * 坏 home 分支为 IgnitionEx.java:1846-1850 "Invalid Ignite installation home folder"）。
     */
    @Test
    public void failedStartFreesRegistrySlot() {
        String name = "failed-then-ok";

        IgniteConfiguration bad = config(name);
        bad.setIgniteHome("/definitely/not/an/ignite/home-" + UUID.randomUUID());
        bad.setWorkDirectory(new File(System.getProperty("java.io.tmpdir")).getAbsolutePath());

        try {
            Ignition.start(bad);

            fail("Expected IgniteException for invalid ignite home");
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Invalid Ignite installation home folder"));
        }

        assertEquals(IgniteState.STOPPED, Ignition.state(name));

        // 坏 home 在失败前已被写进 home 缓存（vendor 同样先 setIgniteHome 再校验）——
        // 复位后重启，模拟运维给出干净环境。
        System.clearProperty(IgniteSystemProperties.IGNITE_HOME);
        U.setIgniteHome(null);

        try (Ignite ignite = Ignition.start(config(name))) {
            assertSame(ignite, Ignition.ignite(name));
        }
    }

    /**
     * stop：注册表摘除、state 转 STOPPED、ignite(name) 转为抛
     * IgniteIllegalStateException、监听器收到 STARTED→STOPPED 两跳
     * （vendor stop 的 fireEvt 分支 + notifyStateChange，IgnitionEx.java:313-334/1393-1401）。
     */
    @Test
    public void stopRemovesInstanceAndNotifiesListener() {
        final List<String> events = new CopyOnWriteArrayList<>();

        IgnitionListener lsnr = new IgnitionListener() {
            @Override public void onStateChange(@Nullable String name, IgniteState state) {
                if ("listener-grid".equals(name))
                    events.add(state.name());
            }
        };

        Ignition.addListener(lsnr);

        try {
            try (Ignite ignored = Ignition.start(config("listener-grid"))) {
                assertEquals(IgniteState.STARTED, Ignition.state("listener-grid"));
            }

            assertEquals(IgniteState.STOPPED, Ignition.state("listener-grid"));

            try {
                Ignition.ignite("listener-grid");

                fail("Expected IgniteIllegalStateException after stop");
            }
            catch (IgniteIllegalStateException ignored) {
                // 预期路径。
            }
        }
        finally {
            Ignition.removeListener(lsnr);
        }

        assertTrue(events.contains("STARTED"));
        assertTrue(!events.isEmpty() && "STOPPED".equals(events.get(events.size() - 1)));
    }

    /**
     * Spring 入口与尚未复刻的入口显式抛 UnsupportedOperationException（ADR 0001）。
     */
    @Test
    public void springAndDeferredEntrypointsThrow() {
        try {
            Ignition.start("config/spring.xml");

            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException ignored) {
            // 预期路径。
        }

        try {
            Ignition.loadSpringBean("config/spring.xml", "bean");

            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException ignored) {
            // 预期路径。
        }

        try {
            Ignition.localIgnite();

            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException ignored) {
            // 预期路径。
        }
    }

    /**
     * 按本地节点 ID 取回实例（vendor grid(UUID) 的注册表遍历，IgnitionEx.java:1248-1267）。
     */
    @Test
    public void igniteByNodeIdRoundTrip() {
        try (Ignite ignite = Ignition.start(config("by-node-id"))) {
            assertSame(ignite, Ignition.ignite(ignite.configuration().getNodeId()));
        }

        try {
            Ignition.ignite(UUID.randomUUID());

            fail("Expected IgniteIllegalStateException for unknown node id");
        }
        catch (IgniteIllegalStateException ignored) {
            // 预期路径。
        }
    }

    /**
     * 记录型测试 logger：捕获 info 消息、可回放 fileName。
     */
    private static final class RecordingLog implements IgniteLogger {
        /** 收到的 info 消息。 */
        final List<String> msgs = new CopyOnWriteArrayList<>();

        /** fileName 回放值。 */
        volatile String fileName;

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            // 无需记录。
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            // 无需记录。
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            msgs.add(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg) {
            msgs.add(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable e) {
            // 无需记录。
        }

        /** {@inheritDoc} */
        @Override public void error(String msg) {
            // 无需记录。
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            // 无需记录。
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String fileName() {
            return fileName;
        }
    }
}
