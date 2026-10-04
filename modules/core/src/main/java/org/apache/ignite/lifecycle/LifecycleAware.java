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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/lifecycle/LifecycleAware.java
// Lesson 0.2 子集：完整复刻（start/stop 两方法即 vendor 全部成员）。
// 0.2 的消费者是 GridLoggerProxy；LifecycleBean 全家（BEFORE/AFTER_NODE_START）是 0.4 的概念簇。

package org.apache.ignite.lifecycle;

/**
 * 需要 start/stop 生命周期的组件实现的接口。
 * <p>
 * kernal 启动期会对配置中所有实现了本接口的组件（logger、SPI、marshaller 等）
 * 依次调用 {@link #start()}；停机期调用 {@link #stop()}。
 */
public interface LifecycleAware {
    /**
     * 启动组件。
     *
     * @throws org.apache.ignite.IgniteException 启动失败时抛出（0.2 复刻签名暂不声明，0.4 对齐）。
     */
    public void start();

    /**
     * 停止组件。
     */
    public void stop();
}
