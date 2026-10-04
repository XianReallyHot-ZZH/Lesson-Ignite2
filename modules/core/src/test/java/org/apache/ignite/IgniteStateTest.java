/*
 * Lesson-Ignite2 自写红绿切片（vendor 无对应物）。
 * 语义来源：vendor IgniteState 枚举序（STARTED/STOPPED/STOPPED_ON_SEGMENTATION/STOPPED_ON_FAILURE）。
 */

package org.apache.ignite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * {@link IgniteState} 枚举值与序数查找的测试。
 */
public class IgniteStateTest {
    /** 枚举序与 vendor 2.18.0 完全一致。 */
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

    /** fromOrdinal 把序数映射回枚举值。 */
    @Test
    public void fromOrdinalMapsBackToValues() {
        assertEquals(IgniteState.STARTED, IgniteState.fromOrdinal((byte)0));
        assertEquals(IgniteState.STOPPED, IgniteState.fromOrdinal((byte)1));
        assertEquals(IgniteState.STOPPED_ON_SEGMENTATION, IgniteState.fromOrdinal((byte)2));
        assertEquals(IgniteState.STOPPED_ON_FAILURE, IgniteState.fromOrdinal((byte)3));
    }

    /** fromOrdinal 对越界序数返回 null。 */
    @Test
    public void fromOrdinalReturnsNullOutOfRange() {
        assertNull(IgniteState.fromOrdinal((byte)-1));
        assertNull(IgniteState.fromOrdinal((byte)4));
        assertNull(IgniteState.fromOrdinal(Byte.MAX_VALUE));
    }
}
