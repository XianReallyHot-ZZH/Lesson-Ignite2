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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/GridProcessorAdapter.java
// Lesson 0.3 子集：ctx/log 持有 + 全部生命周期方法 no-op 默认实现
//（discovery 数据交换类方法随章 2、断连类随章 13 在接口扩展后补 override）。

package org.apache.ignite.internal.processors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;

/**
 * 全部 processor 的高级父适配器。
 */
public abstract class GridProcessorAdapter implements GridProcessor {
    /** kernal 上下文。 */
    protected final GridKernalContext ctx;

    /** grid 日志。 */
    protected final IgniteLogger log;

    /**
     * @param ctx kernal 上下文。
     */
    protected GridProcessorAdapter(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // 无操作。
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // 无操作。
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        // 无操作。
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // 无操作。
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // 无操作。
    }

    /**
     * 参数断言失败时抛统一措辞的异常。
     *
     * @param cond 断言条件。
     * @param condDesc 失败条件描述。
     * @throws IgniteCheckedException 条件为 {@code false} 时抛出。
     */
    protected final void assertParameter(boolean cond, String condDesc) throws IgniteCheckedException {
        if (!cond)
            throw new IgniteCheckedException("Grid configuration parameter invalid: " + condDesc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName();
    }
}
