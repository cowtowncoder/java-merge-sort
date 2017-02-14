package com.fasterxml.sort;

import java.io.IOException;

import com.fasterxml.sort.std.ByteArrayComparator;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;

public class TestLargeSort extends SortTestBase
{
    private static final int STRING_LENGTH = 256;
    private static final int SORT_MEM_BYTES = 1024 * 1024; // 1MB
    private static final int STRING_COUNT = 10 * (SORT_MEM_BYTES / STRING_LENGTH);

    public void testLargeSort() throws IOException {
        Sorter<byte[]> sorter = new Sorter<byte[]>(
            new SortConfig().withMaxMemoryUsage(SORT_MEM_BYTES),
            RawTextLineReader.factory(),
            RawTextLineWriter.factory(),
            new ByteArrayComparator()
        );
        CountingWriter<byte[]> counter = new CountingWriter<byte[]>();
        sorter.sort(new StringGenerator(STRING_COUNT, STRING_LENGTH), counter);
        assertEquals("sorted count", STRING_COUNT, counter.getCount());
        sorter.close();
    }
}
