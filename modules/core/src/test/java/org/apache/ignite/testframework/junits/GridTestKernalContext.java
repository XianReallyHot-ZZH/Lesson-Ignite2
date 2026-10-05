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

// 改写自 vendor: vendors/ignite/modules/core/src/test/java/org/apache/ignite/testframework/junits/GridTestKernalContext.java
// （Apache 2.0；裁剪 plugin/metric/resource 等未到课组件，保留 start/stop 迭代语义）

package org.apache.ignite.testframework.junits;

import java.util.List;
import java.util.ListIterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridKernalGatewayImpl;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteKernal;

/**
 * 测试用 kernal context：按加入顺序 start、逆序 stop。
 */
public class GridTestKernalContext extends GridKernalContextImpl {
    /**
     * @param log 日志。
     */
    public GridTestKernalContext(IgniteLogger log) {
        this(log, new IgniteConfiguration());
    }

    /**
     * @param log 日志。
     * @param cfg 配置。
     */
    public GridTestKernalContext(IgniteLogger log, IgniteConfiguration cfg) {
        super(new IgniteKernal(),
            cfg,
            new GridKernalGatewayImpl(cfg.getIgniteInstanceName()),
            null,
            null);

        config().setGridLogger(log);
    }

    /**
     * 按加入顺序启动全部组件。
     *
     * @throws IgniteCheckedException 启动失败时抛出。
     */
    public void start() throws IgniteCheckedException {
        for (GridComponent comp : components())
            comp.start();
    }

    /**
     * 逆序停止全部组件。
     *
     * @param cancel 是否取消运行中的作业。
     * @throws IgniteCheckedException 停止失败时抛出。
     */
    public void stop(boolean cancel) throws IgniteCheckedException {
        List<GridComponent> comps = components();

        for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
            GridComponent comp = it.previous();

            comp.stop(cancel);
        }
    }
}
