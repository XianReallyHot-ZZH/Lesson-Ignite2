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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * commons 层工具方法。core 的 {@code IgniteUtils} 继承本类。
 */
public abstract class CommonUtils {
    /** 缓存的 Ignite 主目录（vendor 以 GridTuple 缓存，这里直接持引用）。 */
    private static volatile String ggHome;

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
}
