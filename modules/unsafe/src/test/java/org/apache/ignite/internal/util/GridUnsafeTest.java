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
 * {@link GridUnsafe} 静态初始化与公开常量的测试。
 */
public class GridUnsafeTest {
    /** 类加载成功获取 sun.misc.Unsafe 实例。 */
    @Test
    public void staticInitializationAcquiresUnsafe() {
        assertEquals(ByteOrder.nativeOrder(), GridUnsafe.NATIVE_BYTE_ORDER);
    }

    /** BIG_ENDIAN 标志与 JDK 的原生字节序视图一致。 */
    @Test
    public void bigEndianFlagAgreesWithJdk() {
        assertEquals(ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN, GridUnsafe.BIG_ENDIAN);
    }
}
