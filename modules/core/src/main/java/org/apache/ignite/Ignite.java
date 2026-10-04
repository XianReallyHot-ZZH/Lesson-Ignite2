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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/Ignite.java
// Lesson 0.2 子集：标识面四件套（name/log/configuration/close）。
// vendor 接口约 700 行（cluster/compute/cache/transactions/services/messaging/...），
// 按课程纪律逐课生长：未到的 API 不进接口（区别于"进了接口但抛 UnsupportedOperationException"
// —— 那是给已成形子系统的占位方式，见 Ignition 的 Spring 重载）。
// 各方法签名与 javadoc 语义逐条对齐 vendor 原文。

package org.apache.ignite;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Ignite 的主接口。一个 JVM 可以运行多个通过名字区分的 Ignite 实例，
 * 每个实例由 {@link Ignition} 工厂启动并获得本接口的句柄。
 * <p>
 * 本接口 extends {@link AutoCloseable}——{@code try-with-resources} 语句结束时自动停掉实例。
 */
public interface Ignite extends AutoCloseable {
    /**
     * 取 Ignite 实例名。
     * 名字允许在同一个 Java VM 内运行多个不同名字的 Ignite 实例。
     * <p>
     * 若使用默认（无名）实例，返回 {@code null}。
     * 如何启动命名实例参见 {@link Ignition} 文档。
     *
     * @return Ignite 实例名；默认实例返回 {@code null}。
     */
    public String name();

    /**
     * 取本实例的 logger。
     *
     * @return 本实例的 logger。
     */
    public IgniteLogger log();

    /**
     * 取本 Ignite 实例的配置（定稿副本——启动期已完成默认值注入）。
     * <p>
     * 注意：通过本方法取得的 SPI 不应直接使用；SPI 只是子系统的内部视图，
     * 由 Ignite kernal 内部消费。
     *
     * @return Ignite 配置实例。
     */
    public IgniteConfiguration configuration();

    /**
     * 关闭本 grid 实例。等价于 {@code Ignition.stop(igniteInstanceName, true)}。
     * <p>
     * 本方法在 {@code try-with-resources} 语句管理的对象上会被自动调用。
     *
     * @throws IgniteException 停止失败时抛出。
     */
    @Override public void close() throws IgniteException;
}
