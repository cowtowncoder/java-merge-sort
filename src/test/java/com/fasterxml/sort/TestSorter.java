package com.fasterxml.sort;

public class TestSorter extends SortTestBase
{
    public void testMergeRoundCalculation()
    {
        // first arg is number of files, second merge factor
        assertEquals(1, Sorter._calculateRoundCount(3, 4));
        assertEquals(2, Sorter._calculateRoundCount(4, 2));
        assertEquals(8, Sorter._calculateRoundCount(256, 2));
        assertEquals(8, Sorter._calculateRoundCount(129, 2));
        assertEquals(2, Sorter._calculateRoundCount(256, 16));
        assertEquals(2, Sorter._calculateRoundCount(256, 19));
        assertEquals(2, Sorter._calculateRoundCount(5, 4));
    }
}
