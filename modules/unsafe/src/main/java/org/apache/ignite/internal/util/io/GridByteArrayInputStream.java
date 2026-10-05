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

// 对应 vendor: vendors/ignite/modules/unsafe/src/main/java/org/apache/ignite/internal/util/io/GridByteArrayInputStream.java
// Lesson 0.3 子集：无同步的 byte[] 输入流（与 ByteArrayInputStream 等价）。
// 差异：vendor 的块拷贝走 GridUnsafe.arrayCopy（Unsafe 零拷贝路径），复刻用 System.arraycopy
// （纯堆内数组拷贝语义等价）；toString 的 S 反射助手未接入，用默认形态。

package org.apache.ignite.internal.util.io;

import java.io.InputStream;

/**
 * 以 byte 数组为底的无同步输入流。
 */
public class GridByteArrayInputStream extends InputStream {
    /** 底层数组。 */
    private byte buf[];

    /** 当前读位置。 */
    private int pos;

    /** mark 位置。 */
    private int mark;

    /** 有效数据边界。 */
    private int cnt;

    /**
     * @param buf 输入缓冲。
     */
    public GridByteArrayInputStream(byte buf[]) {
        this.buf = buf;

        pos = 0;
        cnt = buf.length;
    }

    /**
     * @param buf 输入缓冲。
     * @param off 首字节偏移。
     * @param len 最大可读字节数。
     */
    public GridByteArrayInputStream(byte buf[], int off, int len) {
        this.buf = buf;

        pos = off;
        cnt = Math.min(off + len, buf.length);
        mark = off;
    }

    /** {@inheritDoc} */
    @Override public int read() {
        return (pos < cnt) ? (buf[pos++] & 0xff) : -1;
    }

    /** {@inheritDoc} */
    @Override public int read(byte b[], int off, int len) {
        if (b == null)
            throw new NullPointerException();
        else if (off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (pos >= cnt)
            return -1;

        if (pos + len > cnt)
            len = cnt - pos;

        if (len <= 0)
            return 0;

        System.arraycopy(buf, pos, b, off, len);

        pos += len;

        return len;
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) {
        if (pos + n > cnt)
            n = cnt - pos;

        if (n < 0)
            return 0;

        pos += n;

        return n;
    }

    /** {@inheritDoc} */
    @Override public int available() {
        return cnt - pos;
    }

    /** {@inheritDoc} */
    @Override public boolean markSupported() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void mark(int readAheadLimit) {
        mark = pos;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() {
        pos = mark;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // 无操作。
    }
}
