/*
 * Lesson-Ignite2 自写红绿切片（vendor 无对应物）。
 * 语义来源：vendor IgniteCheckedException / X.searchForCause 的行为（Apache 2.0，仅作断言依据）。
 */

package org.apache.ignite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * {@link IgniteCheckedException} 构造器与 cause 层级语义的测试。
 */
public class IgniteCheckedExceptionTest {
    /** 仅消息构造器保留消息。 */
    @Test
    public void messageOnlyConstructorKeepsMessage() {
        assertEquals("boom", new IgniteCheckedException("boom").getMessage());
    }

    /** 消息+cause 构造器链上 cause。 */
    @Test
    public void messageAndCauseConstructorChainsCause() {
        Throwable cause = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException("outer", cause);

        assertEquals("outer", e.getMessage());
        assertSame(cause, e.getCause());
    }

    /** 仅 Throwable 构造器从 cause 取消息（vendor 行为）。 */
    @Test
    public void throwableConstructorTakesMessageFromCause() {
        Throwable cause = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException(cause);

        assertEquals("inner", e.getMessage());
        assertSame(cause, e.getCause());
    }

    /** 允许空构造器。 */
    @Test
    public void emptyConstructorAllowed() {
        assertNull(new IgniteCheckedException().getMessage());
    }

    /** 堆栈可写标志生效。 */
    @Test
    public void nonWritableStackTraceProducesEmptyTrace() {
        IgniteCheckedException e = new IgniteCheckedException("boom", null, false);

        assertEquals(0, e.getStackTrace().length);
    }

    /** hasCause 沿 cause 链匹配类。 */
    @Test
    public void hasCauseMatchesNestedCauseClass() {
        IgniteCheckedException e = new IgniteCheckedException("outer",
            new RuntimeException("mid", new IllegalStateException("inner")));

        assertTrue(e.hasCause(IllegalStateException.class));
        assertTrue(e.hasCause(RuntimeException.class));
        assertFalse(e.hasCause(NullPointerException.class));
    }

    /** hasCause 包含异常自身（vendor searchForCause 行为）。 */
    @Test
    public void hasCauseIncludesSelf() {
        IgniteCheckedException e = new IgniteCheckedException("boom");

        assertTrue(e.hasCause(IgniteCheckedException.class));
    }

    /** hasCause 不传类时返回 false。 */
    @Test
    public void hasCauseWithNoClassesReturnsFalse() {
        assertFalse(new IgniteCheckedException("boom").hasCause());
    }

    /** getCause(Class) 返回匹配的嵌套实例本身。 */
    @Test
    public void getCauseByClassReturnsMatchingInstance() {
        IllegalStateException inner = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException("outer",
            new RuntimeException("mid", inner));

        assertSame(inner, e.getCause(IllegalStateException.class));
        assertNull(e.getCause(NullPointerException.class));
    }

    /** toString 渲染类名与消息（vendor 格式）。 */
    @Test
    public void toStringRendersClassAndMessage() {
        assertEquals("class org.apache.ignite.IgniteCheckedException: boom",
            new IgniteCheckedException("boom").toString());
    }
}
