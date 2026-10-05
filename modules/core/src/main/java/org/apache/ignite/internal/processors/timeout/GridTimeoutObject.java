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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/timeout/GridTimeoutObject.java
// Lesson 0.3 子集：完整接口。

package org.apache.ignite.internal.processors.timeout;

import org.apache.ignite.lang.IgniteUuid;

/**
 * 一切可超时对象的接口。
 */
public interface GridTimeoutObject {
    /**
     * @return 对象 ID。
     */
    IgniteUuid timeoutId();

    /**
     * @return 结束时间（绝对毫秒）。
     */
    long endTime();

    /**
     * 超时回调。
     */
    void onTimeout();
}
