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

// 对应 vendor: vendors/ignite/modules/binary/impl/src/main/java/org/apache/ignite/marshaller/jdk/JdkMarshallerOutputStreamWrapper.java
// Lesson 0.3 子集：完整类（不透传 close 的 OutputStream 包装——JDK 序列化流
// 不应顺带关掉调用方的底层流）。

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} 包装器。
 */
class JdkMarshallerOutputStreamWrapper extends OutputStream {
    /** 被包装流。 */
    private OutputStream out;

    /**
     * 创建包装。
     *
     * @param out 被包装输出流。
     */
    JdkMarshallerOutputStreamWrapper(OutputStream out) {
        assert out != null;

        this.out = out;
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        out.write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) throws IOException {
        out.write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        out.flush();
    }
}
