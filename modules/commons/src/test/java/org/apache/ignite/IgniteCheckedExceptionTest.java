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
 * Tests for {@link IgniteCheckedException} constructor and cause-hierarchy semantics.
 */
public class IgniteCheckedExceptionTest {
    /** Message-only constructor keeps the message. */
    @Test
    public void messageOnlyConstructorKeepsMessage() {
        assertEquals("boom", new IgniteCheckedException("boom").getMessage());
    }

    /** Message-and-cause constructor chains the cause. */
    @Test
    public void messageAndCauseConstructorChainsCause() {
        Throwable cause = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException("outer", cause);

        assertEquals("outer", e.getMessage());
        assertSame(cause, e.getCause());
    }

    /** Throwable-only constructor takes message from the cause, mirroring vendor behavior. */
    @Test
    public void throwableConstructorTakesMessageFromCause() {
        Throwable cause = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException(cause);

        assertEquals("inner", e.getMessage());
        assertSame(cause, e.getCause());
    }

    /** Empty constructor is allowed. */
    @Test
    public void emptyConstructorAllowed() {
        assertNull(new IgniteCheckedException().getMessage());
    }

    /** Writable-stack-trace flag is honored. */
    @Test
    public void nonWritableStackTraceProducesEmptyTrace() {
        IgniteCheckedException e = new IgniteCheckedException("boom", null, false);

        assertEquals(0, e.getStackTrace().length);
    }

    /** hasCause matches classes along the cause chain. */
    @Test
    public void hasCauseMatchesNestedCauseClass() {
        IgniteCheckedException e = new IgniteCheckedException("outer",
            new RuntimeException("mid", new IllegalStateException("inner")));

        assertTrue(e.hasCause(IllegalStateException.class));
        assertTrue(e.hasCause(RuntimeException.class));
        assertFalse(e.hasCause(NullPointerException.class));
    }

    /** hasCause includes the exception itself, mirroring vendor searchForCause. */
    @Test
    public void hasCauseIncludesSelf() {
        IgniteCheckedException e = new IgniteCheckedException("boom");

        assertTrue(e.hasCause(IgniteCheckedException.class));
    }

    /** hasCause with no classes returns false. */
    @Test
    public void hasCauseWithNoClassesReturnsFalse() {
        assertFalse(new IgniteCheckedException("boom").hasCause());
    }

    /** getCause(Class) returns the matching nested instance itself. */
    @Test
    public void getCauseByClassReturnsMatchingInstance() {
        IllegalStateException inner = new IllegalStateException("inner");

        IgniteCheckedException e = new IgniteCheckedException("outer",
            new RuntimeException("mid", inner));

        assertSame(inner, e.getCause(IllegalStateException.class));
        assertNull(e.getCause(NullPointerException.class));
    }

    /** toString renders class name and message, mirroring vendor format. */
    @Test
    public void toStringRendersClassAndMessage() {
        assertEquals("class org.apache.ignite.IgniteCheckedException: boom",
            new IgniteCheckedException("boom").toString());
    }
}
