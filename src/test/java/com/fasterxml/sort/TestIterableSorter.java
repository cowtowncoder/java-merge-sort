package com.fasterxml.sort;

import com.fasterxml.sort.util.CollectionReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestIterableSorter extends SortTestBase
{
    private static final List<Integer> INPUT = Arrays.asList(5, 2, 8, 1, 4, 6, 9, 3, 7);
    private static final List<Integer> EXPECTED = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

    public void testBasic() throws IOException {
        IteratingSorter<Integer> sorter = new IteratingSorter<Integer>();
        sortAndCheck("", sorter);
        sorter.close();
    }

    public void testReused() throws IOException {
        IteratingSorter<Integer> sorter = new IteratingSorter<Integer>();
        sortAndCheck("pass 1 ", sorter);
        sortAndCheck("pass 2 ", sorter);
        sortAndCheck("pass 3 ", sorter);
        sorter.close();
    }

    private void sortAndCheck(String msg, IteratingSorter<Integer> sorter) throws IOException {
        Iterator<Integer> sorterIt = sorter.sort(new CollectionReader<Integer>(INPUT));
        assertTrue("sort success", sorterIt != null);
        Iterator<Integer> expectedIt = EXPECTED.iterator();
        int i = 0;
        while(expectedIt.hasNext()) {
            assertTrue(msg + "hasNext on " + i, sorterIt.hasNext());
            assertEquals(msg + "value on " + i, expectedIt.next(), sorterIt.next());
        }
        assertFalse(msg + "extra items", sorterIt.hasNext());
    }
}
