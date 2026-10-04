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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/ShutdownPolicy.java
// Lesson 0.2 子集：完整复刻（枚举两值 + index/fromIndex 往返，vendor 即此成员集）。
// GRACEFUL 路径（等备份再停）依赖 metastore/cache，章 7 接入；0.2 恒走 IMMEDIATE。

package org.apache.ignite;

/**
 * 节点停机策略。
 */
public enum ShutdownPolicy {
    /**
     * 立即停机，不等待备份。
     */
    IMMEDIATE(0),

    /**
     * 优雅停机：确保本地分区在集群其余节点上都有副本后再停。
     */
    GRACEFUL(1);

    /** 序列化用索引（所有版本间保持一致）。 */
    private final int idx;

    /**
     * @param idx 序列化索引。
     */
    ShutdownPolicy(int idx) {
        this.idx = idx;
    }

    /**
     * 取序列化索引。
     *
     * @return 索引。
     */
    public int index() {
        return idx;
    }

    /** 枚举值数组（按索引访问）。 */
    private static final ShutdownPolicy[] VALS;

    static {
        ShutdownPolicy[] plcTypes = ShutdownPolicy.values();

        int maxIdx = 0;

        for (ShutdownPolicy recordType : plcTypes)
            maxIdx = Math.max(maxIdx, recordType.idx);

        VALS = new ShutdownPolicy[maxIdx + 1];

        for (ShutdownPolicy plcType : plcTypes)
            VALS[plcType.idx] = plcType;
    }

    /**
     * 按索引取枚举值（序列化还原用）。
     *
     * @param idx 索引。
     * @return 枚举值；越界时返回 {@code null}。
     */
    public static ShutdownPolicy fromIndex(int idx) {
        return idx >= 0 && idx < VALS.length ? VALS[idx] : null;
    }
}
