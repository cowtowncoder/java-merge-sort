package com.fasterxml.sort;

import java.nio.charset.Charset;

import junit.framework.TestCase;

public abstract class SortTestBase extends TestCase
{
    protected static final Charset CHARSET = Charset.forName("UTF-8");

    protected static class StringGenerator extends DataReader<byte[]> {
        private final int generateCount;
        private final StringBuilder sb;
        private int count;

        public StringGenerator(int generateCount, int stringLength) {
            this.generateCount = generateCount;
            this.sb = new StringBuilder(stringLength);
            for(int i = 0; i < stringLength; ++i) {
                sb.append('a');
            }
        }

        @Override
        public byte[] readNext() {
            if (count >= generateCount) {
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

    protected static class CountingWriter<T> extends DataWriter<T> {
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
}
