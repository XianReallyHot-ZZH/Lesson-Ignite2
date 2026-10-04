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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/util/IgniteUtils.java
// Lesson 0.2 子集：启动路径要用的五件——id8 / awaitQuiet(latch) / initLogger / workDirectory / warn。
// vendor IgniteUtils 约 6300 行（600+ 静态工具），逐课随消费者生长；
// 2.18 拆分后它 extends commons 的 CommonUtils（home 解析与异常转换在基类）。
// initLogger 差异：vendor 先反射探测 classpath 上的 Log4J2Logger（可插拔日志实现）再落 JavaLogger，
// 复刻只带 JavaLogger/NullLogger 两种实现，无 log4j 依赖，直接落 JavaLogger。

package org.apache.ignite.internal.util;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.logger.java.JavaLogger;
import org.jetbrains.annotations.Nullable;

/**
 * core 层工具方法（2.18 起 extends commons 的 {@link CommonUtils}）。
 * 代码里通常以别名 {@link U} 引用。
 */
public abstract class IgniteUtils extends CommonUtils {
    /** 工作目录默认段名（挂在 home 或 user.dir/ignite 下）。 */
    private static final String DEFAULT_WORK_DIR = "work";

    /** 环境变量 IGNITE_WORK_DIR 的进程级快照（与 vendor 一致：只读 env，不读 sysprop）。 */
    public static final String IGNITE_WORK_DIR = System.getenv(IgniteSystemProperties.IGNITE_WORK_DIR);

    /**
     * 工具类不允许实例化。
     */
    protected IgniteUtils() {
        // 空实现。
    }

    /**
     * 取 UUID 的 8 字符短表示（日志用）。
     *
     * @param id 输入 ID。
     * @return 8 字符 ID 子串。
     */
    public static String id8(UUID id) {
        return id.toString().substring(0, 8);
    }

    /**
     * 无中断等待 latch 计数到零：等待期间被中断时先记下标志，返回前恢复中断位。
     *
     * @param latch 要等待的 latch。
     */
    public static void awaitQuiet(CountDownLatch latch) {
        boolean interrupted = false;

        while (true) {
            try {
                latch.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
            finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 初始化 logger：未配置时落 {@link JavaLogger}，JavaLogger 场景补工作目录。
     *
     * @param cfgLog 用户配置的 logger（可为 {@code null}）。
     * @param app 应用名（文件 appender 标记用；IgniteLoggerEx 接入后生效）。
     * @param nodeId 节点 ID（同上）。
     * @param workDir 工作目录。
     * @return 就绪的 logger。
     * @throws IgniteCheckedException 初始化失败时抛出。
     */
    public static IgniteLogger initLogger(
        @Nullable IgniteLogger cfgLog,
        @Nullable String app,
        @Nullable UUID nodeId,
        String workDir
    ) throws IgniteCheckedException {
        try {
            if (cfgLog == null)
                cfgLog = new JavaLogger();

            // JavaLogger 需要工作目录（文件 handler 落日志的位置）。
            if (cfgLog instanceof JavaLogger)
                ((JavaLogger)cfgLog).setWorkDirectory(workDir);

            return cfgLog;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to create logger.", e);
        }
    }

    /**
     * 解析工作目录，优先级与 vendor 一致：
     * 用户显式配置 → 环境变量 {@code IGNITE_WORK_DIR} → {@code IGNITE_HOME/work} →
     * {@code user.dir/ignite/work}（自动创建，并尝试写 README 说明文件）。
     *
     * @param userWorkDir 用户配置的工作目录（可为 {@code null}）。
     * @param userIgniteHome 用户配置的 Ignite home（可为 {@code null}，为空时解析）。
     * @return 绝对工作目录路径。
     * @throws IgniteCheckedException 无法解析或创建失败时抛出。
     */
    public static String workDirectory(@Nullable String userWorkDir, @Nullable String userIgniteHome)
        throws IgniteCheckedException {
        if (userIgniteHome == null)
            userIgniteHome = getIgniteHome();

        File workDir;

        if (!isEmpty(userWorkDir))
            workDir = new File(userWorkDir);
        else if (!isEmpty(IGNITE_WORK_DIR))
            workDir = new File(IGNITE_WORK_DIR);
        else if (!isEmpty(userIgniteHome))
            workDir = new File(userIgniteHome, DEFAULT_WORK_DIR);
        else {
            String userDir = System.getProperty("user.dir");

            if (isEmpty(userDir))
                throw new IgniteCheckedException(
                    "Failed to resolve Ignite work directory. Either IgniteConfiguration.setWorkDirectory or " +
                        "one of the system properties (" + IgniteCommonsSystemProperties.IGNITE_HOME + ", " +
                        IgniteSystemProperties.IGNITE_WORK_DIR + ") must be explicitly set."
                );

            File igniteDir = new File(userDir, "ignite");

            // vendor 会在此创建目录并写 README.txt 说明文件；目录必须建，说明文件暂略。
            igniteDir.mkdirs();

            workDir = new File(igniteDir, DEFAULT_WORK_DIR);
        }

        if (!workDir.isAbsolute())
            throw new IgniteCheckedException("Work directory path must be absolute: " + workDir);

        if (!workDir.mkdirs() && !workDir.isDirectory())
            throw new IgniteCheckedException("Failed to create work directory: " + workDir);

        return workDir.getAbsolutePath();
    }

    /**
     * 输出告警；logger 缺位（注册表操作等无日志可用的场景）时打到标准错误。
     *
     * @param log logger（可为 {@code null}）。
     * @param msg 告警消息。
     */
    public static void warn(@Nullable IgniteLogger log, String msg) {
        if (log == null)
            System.err.println("[WARNING] " + msg);
        else
            log.warning(msg);
    }

    /**
     * 判断字符串是否为 {@code null} 或空。
     * vendor 里是 typedef F.isEmpty 的内联展开——F 随消费者增多再立。
     *
     * @param s 输入。
     * @return {@code null} 或空串返回 {@code true}。
     */
    static boolean isEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }
}
