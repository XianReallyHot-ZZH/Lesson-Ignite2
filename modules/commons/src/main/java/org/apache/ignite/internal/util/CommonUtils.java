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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/CommonUtils.java
// Lesson 0.2 子集：getIgniteHome/setIgniteHome（2.18 拆分后 commons 层的 home 解析）
// + convertException（checked → runtime 转换的最简形态）。
// vendor 版本数千行（文件 IO、字符串、反射、异常转换注册表……），随消费者出现逐个补。
// 差异说明：vendor getIgniteHome 以 GridTuple 缓存并带 resolveProjectHome() classpath 探测，
// 复刻只读 sysprop/环境变量后缓存（无安装目录布局可探测），语义对齐"解析一次后缓存"。

package org.apache.ignite.internal.util;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * commons 层工具方法。core 的 {@code IgniteUtils} 继承本类。
 */
public abstract class CommonUtils {
    /** 缓存的 Ignite 主目录（vendor 以 GridTuple 缓存，这里直接持引用）。 */
    private static volatile String ggHome;

    /** "未设置实例名"哨兵（引用判等专用，勿用 equals 比较——vendor 原注释同此警告）。 */
    public static final String LOC_IGNITE_NAME_EMPTY = new String();

    /** 当前线程的 Ignite 实例名（marshaller 序列化路径读取，区分多实例同 JVM）。 */
    private static final ThreadLocal<String> LOC_IGNITE_NAME = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return LOC_IGNITE_NAME_EMPTY;
        }
    };

    /** 类缓存：loader → (类名 → Class)。vendor 同结构（forName 的 useCache 路径）。 */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    /** Ignite 自身的类加载器（forName 的 loader 兜底）。 */
    private static final ClassLoader gridClassLoader = CommonUtils.class.getClassLoader();

    static {
        // 环境变量/系统属性只解析一次，与 vendor 的惰性单次解析对齐。
        String home = IgniteCommonsSystemProperties.getString(IgniteCommonsSystemProperties.IGNITE_HOME);

        if (home != null && !home.isEmpty())
            ggHome = home;
    }

    /**
     * 工具类不允许实例化。
     */
    protected CommonUtils() {
        // 空实现。
    }

    /**
     * 取 {@code IGNITE_HOME} 属性（先系统属性后环境变量）。
     *
     * @return {@code IGNITE_HOME} 属性值，或无法解析时的 {@code null}。
     */
    @Nullable public static String getIgniteHome() {
        return ggHome;
    }

    /**
     * 设置 {@code IGNITE_HOME} 系统属性并更新缓存（用户在配置里显式给出 home 时调用）。
     *
     * @param path Ignite 主目录路径。
     */
    public static void setIgniteHome(@Nullable String path) {
        if (path != null)
            System.setProperty(IgniteCommonsSystemProperties.IGNITE_HOME, path);

        ggHome = path;
    }

    /**
     * 把 checked 异常转换为 runtime 异常（{@code Ignition.start(...)} 公共入口使用）。
     * <p>
     * vendor 版本先查异常转换注册表（按 IgniteCheckedException 子类映射专属 runtime 异常）、
     * 再特判 client 断连异常，最后兜底 {@code new IgniteException(...)}；注册表与断连异常
     * 分别属于 marshaller/客户端课，当前子集直接走兜底路径。
     *
     * @param e Ignite checked 异常。
     * @return Ignite runtime 异常。
     */
    public static IgniteException convertException(IgniteCheckedException e) {
        return new IgniteException(e.getMessage(), e);
    }

    /**
     * 取当前线程的 Ignite 实例名。
     *
     * @return 实例名（未设置为空串哨兵 {@link #LOC_IGNITE_NAME_EMPTY}）。
     */
    @Nullable public static String getCurrentIgniteName() {
        return LOC_IGNITE_NAME.get();
    }

    /**
     * 判断实例名是否已设置（引用判等——哨兵是专用对象，equals 不可用）。
     *
     * @param name 待检名称。
     * @return 已设置返回 {@code true}。
     */
    @SuppressWarnings("StringEquality")
    public static boolean isCurrentIgniteNameSet(@Nullable String name) {
        return name != LOC_IGNITE_NAME_EMPTY;
    }

    /**
     * 设置当前线程的 Ignite 实例名（marshaller 在每次 marshal/unmarshal 前调用，
     * 使序列化路径能感知"当前为哪个实例工作"）。
     *
     * @param newName 新实例名。
     * @return 旧值（供 finally 恢复）。
     */
    @SuppressWarnings("StringEquality")
    @Nullable public static String setCurrentIgniteName(@Nullable String newName) {
        String oldName = LOC_IGNITE_NAME.get();

        if (oldName != newName)
            LOC_IGNITE_NAME.set(newName);

        return oldName;
    }

    /**
     * 恢复旧实例名（与 {@link #setCurrentIgniteName} 成对使用）。
     *
     * @param oldName 旧实例名。
     * @param curName 当前实例名（相同则无操作）。
     */
    @SuppressWarnings("StringEquality")
    public static void restoreOldIgniteName(@Nullable String oldName, @Nullable String curName) {
        if (oldName != curName)
            LOC_IGNITE_NAME.set(oldName);
    }

    /**
     * 按类名解析 Class，支持类名过滤器与结果缓存。
     * <p>
     * JDK 反序列化路径（{@code JdkMarshallerObjectInputStream.resolveClass}）经此入口，
     * 保证"必须用 {@code Class.forName} 而非 {@code loader.loadClass}"（数组类在某些场景下
     * loadClass 会抛莫名 CNFE——vendor 原注释）。
     *
     * @param clsName 类名。
     * @param ldr 类加载器（{@code null} 用 Ignite 自身 loader）。
     * @param clsFilter 类名过滤器（非 {@code null} 且拒绝时抛 CNFE，防反序列化攻击面）。
     * @param useCache 是否使用类缓存。
     * @return 解析出的 Class。
     * @throws ClassNotFoundException 类不存在或被过滤器拒绝时抛出。
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        @Nullable IgnitePredicate<String> clsFilter,
        boolean useCache
    ) throws ClassNotFoundException {
        assert clsName != null;

        if (!useCache)
            return Class.forName(clsName, true, ldr != null ? ldr : gridClassLoader);

        if (ldr == null)
            ldr = gridClassLoader;

        ConcurrentMap<String, Class> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        Class cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.apply(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            cls = Class.forName(clsName, true, ldr);

            Class old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * 加载 JDK ServiceLoader 服务实现（binary 模块 api/impl 拆分后，
     * {@code Marshallers} 工厂经此发现 impl 模块的实现）。
     *
     * @param svc 服务接口。
     * @return 服务实现迭代器。
     */
    public static <S> Iterable<S> loadService(Class<S> svc) {
        return ServiceLoader.load(svc, gridClassLoader);
    }

    /**
     * 取当前毫秒时间（所有"节点时间"统一入口，方便测试注入时钟——vendor 同点）。
     *
     * @return 当前毫秒数。
     */
    public static long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
