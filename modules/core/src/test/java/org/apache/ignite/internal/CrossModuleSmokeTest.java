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
 * Cross-module smoke test: commons classes are visible from core.
 */
public class CrossModuleSmokeTest {
    /** Commons exception type is usable from core at compile and run time. */
    @Test
    public void commonsExceptionVisibleFromCore() {
        IgniteCheckedException e = new IgniteCheckedException("cross-module",
            new IllegalStateException("nested"));

        assertEquals("cross-module", e.getMessage());
        assertTrue(e.hasCause(IllegalStateException.class));
    }

    /** Core's own public enum is loadable next to commons types. */
    @Test
    public void coreEnumLoadableNextToCommonsTypes() {
        assertEquals(IgniteState.STOPPED, IgniteState.fromOrdinal((byte)1));
    }
}
