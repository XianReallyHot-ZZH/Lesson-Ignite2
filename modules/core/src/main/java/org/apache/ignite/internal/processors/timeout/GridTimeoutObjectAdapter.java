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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/timeout/GridTimeoutObjectAdapter.java
// Lesson 0.3 子集：完整类（负 timeout 折算 Long.MAX_VALUE——永不超时语义）。

package org.apache.ignite.internal.processors.timeout;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * {@link GridTimeoutObject} 的默认实现。
 */
public abstract class GridTimeoutObjectAdapter implements GridTimeoutObject {
    /** 超时 ID。 */
    private final IgniteUuid id;

    /** 结束时间（绝对毫秒）。 */
    private final long endTime;

    /**
     * @param timeout 超时毫秒数（相对当前时间）。
     */
    protected GridTimeoutObjectAdapter(long timeout) {
        this(IgniteUuid.randomUuid(), timeout);
    }

    /**
     * @param id 超时 ID。
     * @param timeout 超时毫秒数。
     */
    protected GridTimeoutObjectAdapter(IgniteUuid id, long timeout) {
        this.id = id;

        long endTime = timeout >= 0 ? U.currentTimeMillis() + timeout : Long.MAX_VALUE;

        this.endTime = endTime >= 0 ? endTime : Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }
}
