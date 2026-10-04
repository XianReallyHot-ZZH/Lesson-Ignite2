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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/IgniteCommonsSystemProperties.java
// Lesson 0.2 子集：取值四件套（getString/getBoolean/getLong）+ IGNITE_HOME 常量。
// core 的 IgniteSystemProperties 继承本类并补充各课用到的常量名。

package org.apache.ignite;

import org.jetbrains.annotations.Nullable;

/**
 * commons 层的系统属性/环境变量访问器。
 * core 模块的 {@code IgniteSystemProperties} 继承本类并定义各子系统常量。
 */
public class IgniteCommonsSystemProperties {
    /** Ignite 安装主目录（sysprop 或环境变量）。 */
    public static final String IGNITE_HOME = "IGNITE_HOME";

    /**
     * 工具类不允许实例化。
     */
    protected IgniteCommonsSystemProperties() {
        // 空实现。
    }

    /**
     * 取系统属性或环境变量（按此顺序查找）。
     *
     * @param name 系统属性或环境变量名。
     * @return 值；两者均未设置时返回 {@code null}。
     */
    @Nullable public static String getString(String name) {
        assert name != null;

        String v = System.getProperty(name);

        if (v == null)
            v = System.getenv(name);

        return v;
    }

    /**
     * 取系统属性或环境变量，未设置时返回默认值。
     *
     * @param name 系统属性或环境变量名。
     * @param dflt 默认值。
     * @return 值或默认值。
     */
    @Nullable public static String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    /**
     * 取布尔型系统属性或环境变量（以 {@code Boolean.valueOf()} 语义解析）。
     *
     * @param name 系统属性或环境变量名。
     * @return 布尔值；未设置时返回 {@code false}。
     */
    public static boolean getBoolean(String name) {
        return getBoolean(name, false);
    }

    /**
     * 取布尔型系统属性或环境变量，未设置时返回默认值。
     *
     * @param name 系统属性或环境变量名。
     * @param dflt 默认值。
     * @return 布尔值或默认值。
     */
    public static boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.parseBoolean(val);
    }

    /**
     * 取长整型系统属性或环境变量，未设置时返回默认值。
     *
     * @param name 系统属性或环境变量名。
     * @param dflt 默认值。
     * @return 长整型值或默认值。
     */
    public static long getLong(String name, long dflt) {
        String val = getString(name);

        return val == null ? dflt : Long.parseLong(val.trim());
    }
}
