package com.fasterxml.sort;

import com.fasterxml.sort.std.ByteArrayComparator;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;

import java.io.IOException;
import java.nio.charset.Charset;

public class TestLargeSort extends SortTestBase
{
    private static final Charset CHARSET =Charset.forName("UTF-8");
    private static final int STRING_LENGTH = 256;
    private static final int SORT_MEM_BYTES = 1024 * 1024; // 1MB
    private static final int STRING_COUNT = 10 * (SORT_MEM_BYTES / STRING_LENGTH);

    private static class StringGenerator extends DataReader<byte[]> {
        private final int generateCount;
        private final StringBuilder sb;
        private int count;

        private StringGenerator(int generateCount, int stringLength) {
            this.generateCount = generateCount;
            this.sb = new StringBuilder(stringLength);
            for(int i = 0; i < stringLength; ++i) {
                sb.append('a');
            }
        }

        @Override
        public byte[] readNext() {
            if(count >= generateCount) {
                return null;
            }
            int saveLen = sb.length();
            sb.append(count++);
            String s = sb.toString();
            sb.setLength(saveLen);
            return s.getBytes(CHARSET);
        }

        @Override
        public int estimateSizeInBytes(byte[] item) {
            return item.length;
        }

        @Override
        public void close() {
            // None
        }
    }

    private static class CountingWriter<T> extends DataWriter<T> {
        private int count = 0;

        public int getCount() {
            return count;
        }

        @Override
        public void writeEntry(T item) {
            ++count;
        }

        @Override
        public void close() {
            // None
        }
    }


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
    }
}
