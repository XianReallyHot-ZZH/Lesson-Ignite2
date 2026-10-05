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

// 对应 vendor: vendors/ignite/modules/binary/impl/src/main/java/org/apache/ignite/marshaller/MarshallersFactoryImpl.java
// Lesson 0.3 子集：jdk() 两方法（optimized() 属部署课 12.x，届时补）。
// 经 META-INF/services/org.apache.ignite.marshaller.MarshallersFactory 注册。

package org.apache.ignite.marshaller;

import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshallerImpl;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MarshallersFactory} 的 impl 模块实现。
 */
public class MarshallersFactoryImpl implements MarshallersFactory {
    /** {@inheritDoc} */
    @Override public org.apache.ignite.marshaller.jdk.JdkMarshaller jdk() {
        return new JdkMarshallerImpl();
    }

    /** {@inheritDoc} */
    @Override public org.apache.ignite.marshaller.jdk.JdkMarshaller jdk(
        @Nullable IgnitePredicate<String> clsFilter) {
        return new JdkMarshallerImpl(clsFilter);
    }
}
