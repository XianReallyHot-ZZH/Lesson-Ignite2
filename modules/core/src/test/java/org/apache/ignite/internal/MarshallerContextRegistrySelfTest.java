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

// Lesson 0.3 自写：MarshallerContextImpl 空注册表切片
//（集群级交换与磁盘持久化属 Lesson 12.3，当前仅本地语义）。

package org.apache.ignite.internal;

import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * MarshallerContextImpl（0.3 空注册表形态）测试。
 */
public class MarshallerContextRegistrySelfTest {
    /** JAVA 平台 ID（vendor MarshallerPlatformIds.JAVA_ID）。 */
    private static final byte JAVA_ID = 0;

    /** 被测上下文。 */
    private final MarshallerContextImpl ctx = new MarshallerContextImpl();

    /**
     * 本地注册 → 按名/按类查询往返。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testRegisterAndLookup() throws Exception {
        String clsName = "java.lang.String";

        int typeId = clsName.hashCode();

        assertTrue(ctx.registerClassNameLocally(JAVA_ID, typeId, clsName));

        assertEquals(clsName, ctx.getClassName(JAVA_ID, typeId));

        assertEquals(clsName, ctx.getClass(typeId, null).getName());
    }

    /**
     * 集群级注册（当前等价本地注册）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testClusterWideRegisterIsLocalForNow() throws Exception {
        String clsName = "java.lang.Integer";

        int typeId = clsName.hashCode();

        assertTrue(ctx.registerClassName(JAVA_ID, typeId, clsName, false));

        assertEquals(clsName, ctx.getClassName(JAVA_ID, typeId));
    }

    /**
     * 未注册 typeId 抛 ClassNotFoundException。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testUnknownTypeRejected() throws Exception {
        try {
            ctx.getClassName(JAVA_ID, "no.such.Type".hashCode());

            fail("Must fail with CNFE");
        }
        catch (ClassNotFoundException e) {
            assertNotNull(e.getMessage());
        }
    }

    /**
     * jdkMarshaller 可用且能往返；系统类型判定当前恒 false（系统类型表随 12.3 接入）。
     *
     * @throws Exception 若失败。
     */
    @Test
    public void testJdkMarshallerAndSystemType() throws Exception {
        JdkMarshaller marsh = ctx.jdkMarshaller();

        assertNotNull(marsh);

        assertEquals("jdk-ctx", marsh.unmarshal(marsh.marshal("jdk-ctx"), null));

        assertFalse(ctx.isSystemType("java.lang.String"));
    }
}
