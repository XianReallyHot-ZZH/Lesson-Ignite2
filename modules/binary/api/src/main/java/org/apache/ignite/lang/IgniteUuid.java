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

// 对应 vendor: vendors/ignite/modules/binary/api/src/main/java/org/apache/ignite/lang/IgniteUuid.java
// Lesson 0.3 子集：UUID + 8 字节计数器的快速 ID。ID 生成/比较/相等/toString 全保留；
// Externalizable 与 Binarylizable 的读写对（依赖 GridIterator/binary writer 线协议）随
// 通信（3.x）与 binary（13.1）课补。

package org.apache.ignite.lang;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.CommonUtils;

/**
 * 性能优于 {@link UUID} 的 ID：在 UUID 基础上追加 8 字节计数器，
 * ID 创建至少快 10 倍（vendor 基测结论）。
 */
public final class IgniteUuid implements Comparable<IgniteUuid>, Cloneable {
    /** 本 JVM 的 VM ID（进程内所有 IgniteUuid 共享）。 */
    public static final UUID VM_ID = UUID.randomUUID();

    /** 计数器发生器（以启动时间播种，避免跨重启碰撞）。 */
    private static final AtomicLong cntGen = new AtomicLong(CommonUtils.currentTimeMillis());

    /** 全局 ID 部分。 */
    private UUID gid;

    /** 本地计数部分。 */
    private long locId;

    /**
     * 构造（按全局 + 本地标识）。
     *
     * @param gid UUID。
     * @param locId 计数器。
     */
    public IgniteUuid(UUID gid, long locId) {
        assert gid != null;

        this.gid = gid;
        this.locId = locId;
    }

    /**
     * @return 本 JVM 的 VM ID。
     */
    public static UUID vmId() {
        return VM_ID;
    }

    /**
     * @return 最后生成的本地 ID。
     */
    public static long lastLocalId() {
        return cntGen.get();
    }

    /**
     * 创建新伪随机 ID。
     *
     * @return 新 ID。
     */
    public static IgniteUuid randomUuid() {
        return new IgniteUuid(VM_ID, cntGen.incrementAndGet());
    }

    /**
     * 基于给定 UUID 构造新 IgniteUuid。
     *
     * @param id UUID 实例。
     * @return 新 ID。
     */
    public static IgniteUuid fromUuid(UUID id) {
        return new IgniteUuid(id, cntGen.getAndIncrement());
    }

    /**
     * @return 本 ID 的短字符串形式（仅 UI 展示用）。
     */
    public String shortString() {
        return new StringBuilder(Long.toHexString(locId)).reverse().toString();
    }

    /**
     * @return 全局 ID 部分。
     */
    public UUID globalId() {
        return gid;
    }

    /**
     * @return 本地 ID 部分。
     */
    public long localId() {
        return locId;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(IgniteUuid o) {
        if (o == this)
            return 0;

        if (o == null)
            return 1;

        int res = Long.compare(locId, o.locId);

        if (res == 0)
            res = gid.compareTo(o.globalId());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof IgniteUuid))
            return false;

        IgniteUuid that = (IgniteUuid)obj;

        return that.locId == locId && that.gid.equals(gid);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * gid.hashCode() + (int)(locId ^ (locId >>> 32));
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return shortString() + '-' + gid.toString();
    }
}
