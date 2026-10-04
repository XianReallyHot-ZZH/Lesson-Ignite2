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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/IgniteSystemProperties.java
// Lesson 0.2 子集：只收录 0.2 起被引用的常量名（vendor 有数百个，逐课随消费者生长）。
// 取值方法（getString/getBoolean/getLong）继承自 commons 的 IgniteCommonsSystemProperties——
// 这是 2.18 模块拆分的直接证据：属性名在 core、取值器在 commons。

package org.apache.ignite;

/**
 * Ignite 系统属性常量与访问器。
 * <p>
 * 每个属性既可从系统属性也可从环境变量读取（先 sysprop 后 env，见基类实现）。
 */
public final class IgniteSystemProperties extends IgniteCommonsSystemProperties {
    /** 覆盖节点 consistentId（测试/运维把拓扑敏感 id 固定下来用）。 */
    public static final String IGNITE_OVERRIDE_CONSISTENT_ID = "IGNITE_OVERRIDE_CONSISTENT_ID";

    /** Ignite 工作目录。 */
    public static final String IGNITE_WORK_DIR = "IGNITE_WORK_DIR";

    /** 配置文件 URL（Spring 入口用；复刻仅保留常量供 start0 记录）。 */
    public static final String IGNITE_CONFIG_URL = "IGNITE_CONFIG_URL";

    /** 静默模式（默认 true：只输出 ERROR 与告警）。 */
    public static final String IGNITE_QUIET = "IGNITE_QUIET";

    /** JavaLogger 是否挂 console appender。 */
    public static final String IGNITE_CONSOLE_APPENDER = "IGNITE_CONSOLE_APPENDER";

    /** 日志消息里附带实例名（兼容旧名 IGNITE_LOG_GRID_NAME）。 */
    public static final String IGNITE_LOG_INSTANCE_NAME = "IGNITE_LOG_INSTANCE_NAME";

    /** 日志消息里附带实例名（旧名，为兼容保留）。 */
    public static final String IGNITE_LOG_GRID_NAME = "IGNITE_LOG_GRID_NAME";

    /** 重启场景下 JVM 退出码的传递属性（脚本重启协议用）。 */
    public static final String IGNITE_RESTART_CODE = "IGNITE_RESTART_CODE";

    /** 重启标记文件路径（脚本重启协议用）。 */
    public static final String IGNITE_SUCCESS_FILE = "IGNITE_SUCCESS_FILE";

    /**
     * 工具类不允许实例化。
     */
    private IgniteSystemProperties() {
        // 空实现。
    }
}
