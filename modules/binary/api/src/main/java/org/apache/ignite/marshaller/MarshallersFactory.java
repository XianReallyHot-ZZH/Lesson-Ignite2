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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/MarshallersFactory.java
// Lesson 0.3 子集：只保留 jdk() 两个方法。vendor 另有 optimized()/optimized(boolean)
// （OptimizedMarshaller 随部署子系统课 12.x 接入时补）。

package org.apache.ignite.marshaller;

import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * 创建 {@link Marshaller} 实现的工厂（SPI——实现由 impl 模块经
 * {@code META-INF/services} 提供，api 模块不依赖 impl）。
 */
public interface MarshallersFactory {
    /**
     * @return 默认 {@link JdkMarshaller} 实例。
     */
    public JdkMarshaller jdk();

    /**
     * @param clsFilter 类过滤器。
     * @return 带类过滤器的 {@link JdkMarshaller} 实例。
     */
    public JdkMarshaller jdk(@Nullable IgnitePredicate<String> clsFilter);
}
