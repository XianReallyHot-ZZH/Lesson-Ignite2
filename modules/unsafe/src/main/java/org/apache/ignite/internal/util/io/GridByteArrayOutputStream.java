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

// 对应 vendor: vendors/ignite/modules/unsafe/src/main/java/org/apache/ignite/internal/util/io/GridByteArrayOutputStream.java
// Lesson 0.3 子集：无同步的 byte[] 输出流（与 ByteArrayOutputStream 等价）。
// 差异：块拷贝用 System.arraycopy（vendor 走 GridUnsafe.arrayCopy，堆内等价）。

package org.apache.ignite.internal.util.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * 以 byte 数组为底的无同步输出流（容量按需倍增）。
 */
public class GridByteArrayOutputStream extends OutputStream {
    /** 数据缓冲。 */
    private byte buf[];

    /** 有效字节数。 */
    private int cnt;

    /**
     * 默认构造（初始容量 32）。
     */
    public GridByteArrayOutputStream() {
        this(32);
    }

    /**
     * @param size 初始容量。
     */
    public GridByteArrayOutputStream(int size) {
        this(size, 0);
    }

    /**
     * @param size 初始容量。
     * @param off 起始写偏移。
     */
    public GridByteArrayOutputStream(int size, int off) {
        if (size < 0)
            throw new IllegalArgumentException("Negative initial size: " + size);

        if (off > size)
            throw new IllegalArgumentException("Invalid offset: " + off);

        buf = new byte[size];

        cnt = off;
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        int newCnt = cnt + 1;

        if (newCnt > buf.length)
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newCnt));

        buf[cnt] = (byte)b;

        cnt = newCnt;
    }

    /** {@inheritDoc} */
    @Override public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0))
            throw new IndexOutOfBoundsException();
        else if (len == 0)
            return;

        int newCnt = cnt + len;

        if (newCnt > buf.length)
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newCnt));

        System.arraycopy(b, off, buf, cnt, len);

        cnt = newCnt;
    }

    /**
     * 把全部有效内容写入另一个输出流。
     *
     * @param out 目标流。
     * @throws IOException 写入失败时抛出。
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, cnt);
    }

    /**
     * 重置计数（复用已分配缓冲）。
     */
    public void reset() {
        cnt = 0;
    }

    /**
     * 取内部数组（无拷贝，谨慎使用）。
     *
     * @return 内部数组。
     */
    public byte[] internalArray() {
        return buf;
    }

    /**
     * 拷贝出有效内容。
     *
     * @return 内容副本。
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(buf, cnt);
    }

    /**
     * @return 有效字节数。
     */
    public int size() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // 无操作。
    }
}
