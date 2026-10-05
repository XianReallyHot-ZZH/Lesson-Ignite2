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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/marshaller/AbstractMarshaller.java
// Lesson 0.3 子集：完整类（context 持有 + 默认缓冲大小常量）。

package org.apache.ignite.marshaller;

/**
 * marshaller 基类：持有 marshaller 上下文，提供 byte 数组实现的公共默认值。
 */
public abstract class AbstractMarshaller implements Marshaller {
    /** byte 数组输出流的默认初始缓冲大小。 */
    public static final int DFLT_BUFFER_SIZE = 512;

    /** 上下文。 */
    protected MarshallerContext ctx;

    /**
     * @return marshaller 上下文。
     */
    public MarshallerContext getContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public void setContext(MarshallerContext ctx) {
        this.ctx = ctx;
    }
}
