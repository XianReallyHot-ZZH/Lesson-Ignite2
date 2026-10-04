/*
 * Lesson-Ignite2 自写红绿切片（vendor 无对应物）。
 * 语义来源：vendor IgniteState 枚举序（STARTED/STOPPED/STOPPED_ON_SEGMENTATION/STOPPED_ON_FAILURE）。
 */

package org.apache.ignite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Tests for {@link IgniteState} enumeration values and ordinal lookup.
 */
public class IgniteStateTest {
    /** Enumeration order mirrors vendor 2.18.0 exactly. */
    @Test
    public void enumOrderMirrorsVendor() {
        IgniteState[] expected = {
            IgniteState.STARTED,
            IgniteState.STOPPED,
            IgniteState.STOPPED_ON_SEGMENTATION,
            IgniteState.STOPPED_ON_FAILURE
        };

        assertEquals(expected.length, IgniteState.values().length);

        for (int i = 0; i < expected.length; i++)
            assertEquals(expected[i], IgniteState.values()[i]);
    }

    /** fromOrdinal maps ordinals back to values. */
    @Test
    public void fromOrdinalMapsBackToValues() {
        assertEquals(IgniteState.STARTED, IgniteState.fromOrdinal((byte)0));
        assertEquals(IgniteState.STOPPED, IgniteState.fromOrdinal((byte)1));
        assertEquals(IgniteState.STOPPED_ON_SEGMENTATION, IgniteState.fromOrdinal((byte)2));
        assertEquals(IgniteState.STOPPED_ON_FAILURE, IgniteState.fromOrdinal((byte)3));
    }

    /** fromOrdinal returns null for out-of-range ordinals. */
    @Test
    public void fromOrdinalReturnsNullOutOfRange() {
        assertNull(IgniteState.fromOrdinal((byte)-1));
        assertNull(IgniteState.fromOrdinal((byte)4));
        assertNull(IgniteState.fromOrdinal(Byte.MAX_VALUE));
    }
}
