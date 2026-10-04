/*
 * Lesson 0.1 tracer 验收：commons→core 跨模块冒烟。
 * core 以 compile 依赖 commons（provided+shade 协议的底座一侧），
 * 本测试证明 commons 的类在 core 的编译期与运行期都可用。
 */

package org.apache.ignite.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteState;
import org.junit.Test;

/**
 * 跨模块冒烟测试：commons 的类在 core 可见。
 */
public class CrossModuleSmokeTest {
    /** commons 的异常类型在 core 的编译期与运行期均可用。 */
    @Test
    public void commonsExceptionVisibleFromCore() {
        IgniteCheckedException e = new IgniteCheckedException("cross-module",
            new IllegalStateException("nested"));

        assertEquals("cross-module", e.getMessage());
        assertTrue(e.hasCause(IllegalStateException.class));
    }

    /** core 自己的公共枚举可与 commons 类型一同加载。 */
    @Test
    public void coreEnumLoadableNextToCommonsTypes() {
        assertEquals(IgniteState.STOPPED, IgniteState.fromOrdinal((byte)1));
    }
}
