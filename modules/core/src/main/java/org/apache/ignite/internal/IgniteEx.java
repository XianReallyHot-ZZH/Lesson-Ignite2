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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgniteEx.java
// Lesson 0.3 子集：context() 访问器（kernal 容器对测试与内部代码暴露的唯一面）。
// vendor 另有的 utilityCache/cachex/cluster()/localNode()/commandsRegistry() 等扩展面
// 随 cache（章 1）、cluster（章 2）等课程接入。

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;

/**
 * 扩展 Grid 接口：kernal 与测试所需的附加方法。
 */
public interface IgniteEx extends Ignite {
    /**
     * 内部上下文。
     *
     * @return kernal 上下文。
     */
    public GridKernalContext context();
}
