/*
 * Lesson-Ignite2 自写红绿切片（vendor 无对应物）。
 * 断言点：类加载触发静态初始化（unsafe() 反射获取 theUnsafe 成功），
 * 公开常量 NATIVE_BYTE_ORDER / BIG_ENDIAN 与 JDK 视图一致（vendor 同名同语义）。
 */

package org.apache.ignite.internal.util;

import static org.junit.Assert.assertEquals;

import java.nio.ByteOrder;
import org.junit.Test;

/**
 * Tests for {@link GridUnsafe} static initialization and public constants.
 */
public class GridUnsafeTest {
    /** Class load acquires the sun.misc.Unsafe instance without failure. */
    @Test
    public void staticInitializationAcquiresUnsafe() {
        assertEquals(ByteOrder.nativeOrder(), GridUnsafe.NATIVE_BYTE_ORDER);
    }

    /** BIG_ENDIAN flag agrees with the JDK view of native order. */
    @Test
    public void bigEndianFlagAgreesWithJdk() {
        assertEquals(ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN, GridUnsafe.BIG_ENDIAN);
    }
}
