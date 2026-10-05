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

// 改写自 vendor: vendors/ignite/modules/core/src/test/java/org/apache/ignite/marshaller/GridMarshallerAbstractTest.java
// （Apache 2.0；本轮往返/异常/排除切片取 JdkMarshaller 相关部分，断言风格保留 vendor 原貌）
// Lesson 0.3：JdkMarshaller 完整实现的 tracer 测试——marshal/unmarshal 往返点亮。

package org.apache.ignite.marshaller.jdk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * JdkMarshaller 往返与边界测试。
 */
public class JdkMarshallerSelfTest {
    /** 被测 marshaller。 */
    private JdkMarshaller marsh;

    /** */
    @Before public void setUp() {
        marsh = Marshallers.jdk();

        MarshallerExclusions.clearCache();
    }

    /** */
    @After public void tearDown() {
        MarshallerExclusions.clearCache();
    }

    /**
     * 测试 bean。
     */
    private static class TestBean implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String s;

        /** */
        private int i;

        /** */
        private double[] arr;

        /** */
        private TestBean child;

        /**
         * @param s 字符串字段。
         * @param i 整型字段。
         * @param arr 数组字段。
         */
        private TestBean(String s, int i, double[] arr) {
            this.s = s;
            this.i = i;
            this.arr = arr;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestBean))
                return false;

            TestBean that = (TestBean)o;

            return i == that.i && (s != null ? s.equals(that.s) : that.s == null)
                && java.util.Arrays.equals(arr, that.arr)
                && (child != null ? child.equals(that.child) : that.child == null);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestBean [s=" + s + ", i=" + i + ", arr=" + java.util.Arrays.toString(arr) + ']';
        }
    }

    /**
     * @throws Exception 若失败。
     */
    @Test
    public void testRoundTripNull() throws Exception {
        assertNull(marsh.unmarshal(marsh.marshal(null), null));
    }

    /**
     * @throws Exception 若失败。
     */
    @Test
    public void testRoundTripString() throws Exception {
        assertEquals("test", marsh.unmarshal(marsh.marshal("test"), null));
    }

    /**
     * @throws Exception 若失败。
     */
    @Test
    public void testRoundTripBean() throws Exception {
        TestBean bean = new TestBean("a", 1, new double[]{1.1, 2.2, 3.3});

        bean.child = new TestBean("b", 2, null);

        TestBean cp = marsh.unmarshal(marsh.marshal(bean), null);

        assertEquals(bean, cp);
    }

    /**
     * 流形态与 byte[] 形态必须产出相同字节（vendor 两套 marshal 委托同一实现）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testStreamSameAsBytes() throws Exception {
        byte[] viaBytes = marsh.marshal("stream-check");

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        marsh.marshal("stream-check", out);

        assertArrayEquals(viaBytes, out.toByteArray());

        assertEquals("stream-check", marsh.unmarshal(
            new ByteArrayInputStream(out.toByteArray()), null));
    }

    /**
     * 工厂必须经 ServiceLoader 从 impl 模块加载实现。
     */
    @Test
    public void testFactoryLoadsImpl() {
        assertNotNull(marsh);

        assertTrue(marsh instanceof JdkMarshallerImpl);
    }

    /**
     * 反序列化找不到类时抛 IgniteCheckedException（cause 为 ClassNotFoundException）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testClassNotFound() throws Exception {
        TestBean bean = new TestBean("x", 9, null);

        byte[] bytes = marsh.marshal(bean);

        // 无法加载 TestBean 的类加载器（独立空加载器，双亲委派到 bootstrap 找不到测试类）。
        ClassLoader empty = new ClassLoader(null) { };

        try {
            marsh.unmarshal(bytes, empty);

            fail("Unmarshal must fail with missing class");
        }
        catch (IgniteCheckedException e) {
            assertTrue("Cause must be CNFE: " + e.getCause(),
                e.getCause() instanceof ClassNotFoundException);
        }
    }

    /**
     * 类名过滤器拒绝的类型在反序列化时被拒绝。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testClassFilter() throws Exception {
        JdkMarshaller filtered = Marshallers.jdk(
            clsName -> !clsName.equals(TestBean.class.getName()));

        byte[] bytes = filtered.marshal(new TestBean("secret", 1, null));

        try {
            filtered.unmarshal(bytes, null);

            fail("Filter must reject TestBean");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

    /**
     * 被排除（exclude 登记的父类型）的实例序列化为 null。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testExcludedClassSerializedAsNull() throws Exception {
        MarshallerExclusions.exclude(ExcludedBean.class);

        assertNull(marsh.unmarshal(marsh.marshal(new ExcludedBean()), null));
    }

    /**
     * 普通 Object 实例序列化后反序列化仍是 Object（Dummy 替身逻辑不破坏普通对象）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testPlainObjectRoundTrip() throws Exception {
        Object cp = marsh.unmarshal(marsh.marshal(new Object()), null);

        assertNotNull(cp);

        assertSame(Object.class, cp.getClass());
    }

    /**
     * 排除测试专用 bean。
     */
    private static class ExcludedBean implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;
    }
}
