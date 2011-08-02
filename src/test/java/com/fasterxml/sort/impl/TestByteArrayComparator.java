package com.fasterxml.sort.impl;

import com.fasterxml.sort.SortTestBase;

public class TestByteArrayComparator
    extends SortTestBase
{
    public void testSimple()
    {
        ByteArrayComparator cmp = new ByteArrayComparator();
        // simple equality
        assertEquals(0, cmp.compare(new byte[] { }, new byte[] { }));
        assertEquals(0, cmp.compare(new byte[] { 1, 2 }, new byte[] { 1, 2 }));

        // longer vs shorter
        assertEquals(-1, cmp.compare(new byte[] { 1, 2 }, new byte[] { 1, 2, 3}));
        assertEquals(1, cmp.compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }));

        // then comparisons with normal signed values
        assertEquals(1, cmp.compare(new byte[] { 1, 2 }, new byte[] { 1, 1 }));
        assertEquals(-1, cmp.compare(new byte[] { 1, 1 }, new byte[] { 1, 2 }));

        // and finally ensure that we ignore "signed-ness" of bytes
        assertTrue(cmp.compare(new byte[] { 1, (byte) 0xFF }, new byte[] { 1, 1 }) > 0);
        assertTrue(cmp.compare(new byte[] { 1, 1 }, new byte[] { 1, (byte) 0xFF }) < 0);
    }
}
